use std::env;
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::prelude::*;

mod iceberg_table;

mod file_io;
use crate::file_io::FileIO;

mod file_catalog;
use crate::file_catalog::FileCatalog;


#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let table_loc = &args[1];

    let file_io = FileIO {};
    let file_cat = FileCatalog::new(Arc::new(file_io));

    let table = file_cat.load_table(table_loc).await?;
    println!("{:?}", table.location());

    println!("current snapshot is {}", table.current_snapshot().unwrap().snapshot_id);
    println!("{:?}", table.current_snapshot());
    println!("{:?}", table.current_snapshot().unwrap().manifest_list);
    //table.refresh().await?;
    println!("current snapshot is {}", table.current_snapshot().unwrap().snapshot_id);

    let snapshot_ids: Vec<u64> = table.snapshots().unwrap().into_iter().map(|s| s.snapshot_id).collect();
    println!("{:?}", snapshot_ids);

    //println!("{:?}", table.schema());

    // Datafusion
    let ctx = SessionContext::new();
    ctx.register_table("demo", Arc::new(table)).unwrap();

    let query_string = "SELECT * FROM demo WHERE adouble > 0 ORDER BY afloat DESC LIMIT 10";
    let df = ctx.sql(query_string).await?;
    df.show().await?;

    ctx.sql("SELECT COUNT(*) AS rowcount FROM demo").await?.show().await?;

    Ok(())
}
