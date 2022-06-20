use futures::future::try_join_all;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::convert::TryFrom;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit};
use datafusion::arrow::error::ArrowError;

use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion_data_access::object_store::local::LocalFileSystem;
use datafusion::error::Result;
use datafusion::physical_plan::file_format::FileScanConfig;
use datafusion::physical_plan::Statistics;

use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::{TableProvider,TableType};

use crate::fileio::FileIO;

#[allow(dead_code)]
fn print_type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>())
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
struct IcebergField {
    id: u64,
    name: String,
    required: bool,
    #[serde(rename = "type")]
    field_type: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
struct IcebergPartitionField {
    source_id: u64,
    field_id: u64,
    name: String,
    transform: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
struct IcebergSortField {
    source_id: u64,
    transform: String,
    direction: String,
    null_order: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
struct IcebergSchema {
    #[serde(rename = "type")]
    schema_type: String,
    schema_id: u64,
    fields: Vec<IcebergField>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
struct IcebergPartitionSpec {
    spec_id: u64,
    fields: Vec<IcebergPartitionField>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
struct IcebergSortOrder {
    order_id: u64,
    fields: Vec<IcebergSortField>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct IcebergSnapshotLog {
    timestamp_ms: u64,
    snapshot_id: u64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
struct IcebergMetadataLog {
    timestamp_ms: u64,
    metadata_file: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
struct IcebergSnapshotSummary {
    operation: String,
    #[serde(rename = "spark.app.id")]
    spark_app_id: String,
    added_data_files: String,
    added_records: String,
    added_files_size: String,
    changed_partition_count: String,
    total_records: String,
    total_files_size: String,
    total_data_files: String,
    total_delete_files: String,
    total_position_deletes: String,
    total_equality_deletes: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct IcebergSnapshot {
    pub snapshot_id: u64,
    parent_snapshot_id: Option<u64>,
    timestamp_ms: u64,
    //summary: IcebergSnapshotSummary,
    pub manifest_list: String,
    schema_id: u64,
}


#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
struct IcebergMetadata {
    format_version: u32,
    table_uuid: String,
    location: String,
    last_updated_ms: u64,
    last_column_id: u64,
    schemas: Vec<IcebergSchema>,
    current_schema_id: u64,
    partition_spec: Vec<IcebergPartitionField>,
    default_spec_id: u64,
    partition_specs: Vec<IcebergPartitionSpec>,
    last_partition_id: u64,
    default_sort_order_id: u64,
    sort_orders: Vec<IcebergSortOrder>,
    //properties
    current_snapshot_id: i64,
    snapshots: Vec<IcebergSnapshot>,
    snapshot_log: Vec<IcebergSnapshotLog>,
    metadata_log: Vec<IcebergMetadataLog>,
}


#[derive(Serialize, Deserialize, Debug)]
struct IcebergManifestEntry {
    status: u64,
}

impl TryFrom<&IcebergSchema> for ArrowSchema {
    type Error = ArrowError;

    fn try_from(s: &IcebergSchema) -> core::result::Result<Self, Self::Error> {
        let fields = s.fields.iter()
            .map(<ArrowField as TryFrom<&IcebergField>>::try_from)
            .collect::<core::result::Result<Vec<ArrowField>, Self::Error>>()?;
        Ok(ArrowSchema::new(fields))
    }
}

impl TryFrom<&IcebergField> for ArrowField {
    type Error = ArrowError;

    fn try_from(f: &IcebergField) -> core::result::Result<Self, Self::Error> {
        Ok(ArrowField::new(&f.name, convert_iceberg_type_to_arrow(&f.field_type)?, !f.required))
    }
}

fn convert_iceberg_type_to_arrow(iceberg_type: &str) -> core::result::Result<ArrowDataType, ArrowError> {
    match iceberg_type {
        "boolean" => Ok(ArrowDataType::Boolean),
        "int" => Ok(ArrowDataType::Int32),
        "long" => Ok(ArrowDataType::Int64),
        "float" => Ok(ArrowDataType::Float32),
        "double" => Ok(ArrowDataType::Float64),
        "timestamptz" => Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".to_string()))),
        "string" => Ok(ArrowDataType::Utf8),
        "binary" => Ok(ArrowDataType::Binary),
        _ => Ok(ArrowDataType::Null),
    }
}

pub struct IcebergTable {
    io: Arc<FileIO>,
    location: String,

    metadata: Option<IcebergMetadata>,

    current_manifest_paths: Vec<String>,
    datafiles: Vec<IcebergDataFile>,
}

impl IcebergTable {
    pub fn new(io: Arc<FileIO>, location: &str) -> Self {
        IcebergTable { io, location: location.to_string(), metadata: None, current_manifest_paths: vec![], datafiles: vec![] }
    }

    pub fn location(&self) -> &str {
        &self.location
    }

    fn get_uri_absolute(&self, path: &str) -> String {
        let meta = &self.metadata.as_ref().unwrap();
        str::replace(path, &meta.location, self.location())
    }

    pub fn snapshots(&self) -> Option<&Vec<IcebergSnapshot>> {
        match &self.metadata {
            Some(t) => Some(&t.snapshots),
            None => None,
        }
    }

    async fn read_data_files_from_manifest(&self, manifest_path: String) -> Result<Vec<IcebergDataFile>> {
        let contents = self.io.new_input(&manifest_path).read_all().await?;
        Ok(extract_files_from_manifest(contents.as_slice()))
    }

    // Return positive integer version hint, or 0 for any errors
    async fn get_version_hint(&self, p: &str) -> i64 {
        let contents = match self.io.new_input(p).read_to_string().await {
            Ok(b) => b,
            Err(_) => return 0,
        };
         
        match contents.parse() {
            Ok(n) if n > 0 => n,
            Ok(_) => 1,
            Err(_) => 1,
        }
    }

    async fn get_latest_table_version(&self) -> String {
        let hintfile_path = Path::new(self.location()).join("metadata").join("version-hint.text");
        let hint = self.get_version_hint(&hintfile_path.into_os_string().into_string().unwrap()).await;

        let p = Path::new(self.location()).join("metadata").join("v".to_string() + &hint.to_string() + ".metadata.json");
        let pathstring = p.into_os_string().into_string().unwrap();

        match self.io.new_input(&pathstring).read_to_string().await {
            Ok(s) => s,
            Err(e) => panic!("Did not find a current table version, I don't want to handle this case yet. {}", e),
        }
    }

    pub async fn refresh(&mut self) -> std::io::Result<()> {
        let contents = self.get_latest_table_version().await;
        self.metadata = match serde_json::from_str(&contents) {
            Ok(m) => Some(m),
            Err(e) => return Err(std::io::Error::new::<serde_json::Error>(std::io::ErrorKind::InvalidData, e.into())),
        };

        let meta = &self.metadata.as_ref().unwrap();

        let manifest_path = str::replace(&self.current_snapshot().unwrap().manifest_list, &meta.location, self.location());
        println!("manifest list path = {}", manifest_path);

        let contents = self.io.new_input(&manifest_path).read_all().await?;
        self.current_manifest_paths = extract_manifest_list(contents.as_slice());

        let mut read_reqs = vec![];
        for rawpath in &self.current_manifest_paths {
            //for row in 0..batch.num_rows() {
            //    let col = batch.column(0);
            //    let array = col.as_any().downcast_ref::<array::StringArray>().unwrap();

            let manifest_path = str::replace(rawpath, &meta.location, self.location());
            println!("reading manifest @ {}", manifest_path);
            read_reqs.push(self.read_data_files_from_manifest(manifest_path))
        }

        let results = try_join_all(read_reqs).await.unwrap();
        self.datafiles = results.into_iter().flatten().collect();
        //self.current_datafile_paths = results.into_iter().flatten().map(|p| self.get_uri_absolute(&p)).collect();

        println!("Found {} data files.", self.datafiles.len());

        Ok(())
    }

    pub fn current_snapshot(&self) -> Option<&IcebergSnapshot> {
        match &self.metadata {
            Some(m) => m.snapshots.iter().find(|&s| s.snapshot_id == m.current_snapshot_id as u64),
            None => None,
        }
    }
}

fn extract_manifest_list(f: impl std::io::Read) -> Vec<String> {
    let mut manifestlist = Vec::new();

    let reader = avro_rs::Reader::new(f).unwrap();
    for value in reader {
        let val = value.unwrap();
        if let avro_rs::types::Value::Record(record) = val {
            //for r in &record {
            //    print_type_of(&r);
            //    println!("{:?}", r);
            //}
            let files_itr = record.into_iter().find(|x| x.0 == "manifest_path").unwrap().1;
            if let avro_rs::types::Value::String(path) = files_itr {
                manifestlist.push(path);
            }
        }
    }
    manifestlist
}

#[derive(Debug)]
struct IcebergDataFile {
    file_path: String,
    file_format: String,
    file_size_in_bytes: i64,
    record_count: i64,
}


fn extract_files_from_manifest(f: impl std::io::Read) -> Vec<IcebergDataFile> {
    let mut filelist = Vec::new();
    let reader = avro_rs::Reader::new(f).unwrap();
    for value in reader {
        let val = value.unwrap();
        //print_type_of(&val);
        //println!("{:?}", val);
        if let avro_rs::types::Value::Record(record) = val {
            //for r in &record {
            //    print_type_of(&r);
            //    println!("{:?}", r);
            //}
            let files_itr = record.into_iter().find(|x| x.0 == "data_file").unwrap().1;
            //print_type_of(&files_itr);
            //println!("data_file {:?}", files_itr);
            
            if let avro_rs::types::Value::Record(inner_record) = files_itr {
                //for d in &inner_record {
                //    println!("{:?}", d);
                //}
                //println!("----");

                let itr = &inner_record.iter().find(|x| x.0 == "file_path").unwrap().1;
                let file_path = match itr {
                    avro_rs::types::Value::String(f) => f.to_string(),
                    _ => "".to_string(),
                };
                
                let itr = &inner_record.iter().find(|x| x.0 == "file_format").unwrap().1;
                let file_format = match itr {
                    avro_rs::types::Value::String(f) => f.to_string(),
                    _ => "".to_string(),
                };
                let itr = &inner_record.iter().find(|x| x.0 == "file_size_in_bytes").unwrap().1;
                let file_size_in_bytes = match itr {
                    avro_rs::types::Value::Long(x) => *x,
                    _ => 0,
                };
                let itr = &inner_record.iter().find(|x| x.0 == "record_count").unwrap().1;
                let record_count = match itr {
                    avro_rs::types::Value::Long(x) => *x,
                    _ => 0,
                };
                let data_file = IcebergDataFile{file_path, file_format, file_size_in_bytes, record_count};
                if data_file.file_format != "PARQUET" {
                    println!("Unexpected file format: {}", data_file.file_format);
                }
                filelist.push(data_file);
            }
        }
    }
    filelist
}


#[async_trait]
impl TableProvider for IcebergTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        let meta = self.metadata.as_ref().unwrap();
        let s = meta.schemas.iter().find(|&s| s.schema_id == meta.current_schema_id).unwrap();

        let arrow_schema = match ArrowSchema::try_from(s) {
            Ok(s) => s,
            Err(_) => ArrowSchema::empty(),
        };
        Arc::new(arrow_schema)
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[datafusion::logical_plan::Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let partitions = self.datafiles
            .iter()
            .map(|f| {
                Ok(vec![PartitionedFile::new(self.get_uri_absolute(&f.file_path), f.file_size_in_bytes as u64)])
            })
            .collect::<datafusion::error::Result<_>>()?;

        let df_object_store = Arc::new(LocalFileSystem {});

        let num_rows = Some(self.datafiles.iter().map(|f| &f.record_count).fold(0, |sum, x| sum + *x as usize));
        let total_byte_size = Some(self.datafiles.iter().map(|f| &f.file_size_in_bytes).fold(0, |sum, x| sum + x) as usize);

        let stats = Statistics{ num_rows, total_byte_size, column_statistics: None, is_exact: false };

        let partition_cols: Vec<String> = vec![];

        ParquetFormat::default()
            .create_physical_plan(FileScanConfig {
                    object_store: df_object_store,
                    file_schema: self.schema(),
                    file_groups: partitions,
                    statistics: stats,
                    projection: projection.clone(),
                    limit,
                    table_partition_cols: partition_cols,
            }, filters).await
    }
}

