use std::env;
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::prelude::*;

mod iceberg_table;
use crate::iceberg_table::IcebergTable;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let table_loc = &args[1];
   
    let table = IcebergTable::load(table_loc.to_string()).await?;
    println!("{:?}", table.location());

    println!("current snapshot is {}", table.current_snapshot().unwrap().snapshot_id);
    println!("{:?}", table.current_snapshot());
    println!("{:?}", table.current_snapshot().unwrap().manifest_list);

    //println!("{:?}", table.schema());

    // Datafusion
    let mut ctx = ExecutionContext::new();
    ctx.register_table("demo", Arc::new(table)).unwrap();
    
    let df = ctx.sql("SELECT * FROM demo LIMIT 10").await?;
    df.show().await?;

    ctx.sql("SELECT COUNT(*) AS rowcount FROM demo").await?.show().await?;

    Ok(())
}
