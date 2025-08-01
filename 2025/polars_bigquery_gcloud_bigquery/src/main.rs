use gcloud_bigquery::client::{ClientConfig, Client};
use tokio;

use gcloud_bigquery::storage::row::Row;
use gcloud_bigquery::http::table::TableReference;

async fn run(client: &Client) {
    let table = TableReference {
        project_id: "bigquery-public-data".to_string(),
        dataset_id: "usa_names".to_string(),
        table_id: "usa_1910_2013".to_string(),
    };
    let mut iter = client.read_table::<Row>(&table, None).await.unwrap();
    while let Some(row) = iter.next().await.unwrap() {
        let col1 = row.column::<String>(0);
        let col2 = row.column::<Option<String>>(1);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    let config = Config::new(&args).unwrap_or_else(|err| {
        println!("Problem parsing arguments: {}", err);
        process::exit(1);
    });
    match label_printer::start_pos_listener(config) {
        Ok(x) => x,
        Err(_) => println!("Listener failure"),
    };

    let (config, project_id) = ClientConfig::new_with_auth().await.unwrap();
    let client = Client::new(config).await.unwrap();

    Ok(())
}

