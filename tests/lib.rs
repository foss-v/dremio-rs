use dremio_rs::Client as DremioClient;
use reqwest::Client as HttpClient;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage,
};

#[tokio::test]
async fn test_dremio() {
    let container = GenericImage::new("dremio/dremio-oss", "latest")
        .with_exposed_port(9047.tcp())
        .with_exposed_port(31010.tcp())
        .with_exposed_port(32010.tcp())
        .with_exposed_port(45678.tcp())
        .with_wait_for(WaitFor::message_on_stdout(
            "com.dremio.dac.server.DremioServer - Started on http://localhost:9047",
        ))
        .start()
        .await
        .expect("Failed to start Dremio container");
    let rest_url = format!(
        "http://localhost:{}",
        container.get_host_port_ipv4(9047).await.unwrap()
    );
    let http_client = HttpClient::new();
    let user = "dremio";
    let pass = "dremio123";
    let body = format!(
        r#"{{"userName":"{}","firstName":"{}","lastName":"{}","email":"{}","createdAt":1526186430755,"password":"{}"}}"#,
        user, user, user, user, pass
    );
    let res = http_client
        .put(format!("{}/apiv2/bootstrap/firstuser", rest_url))
        .header("Authorization", "_dremionull")
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .expect("Failed to create first user");
    if !res.status().is_success() {
        panic!("Failed to create first user");
    }
    let flight_url = format!(
        "grpc://localhost:{}",
        container.get_host_port_ipv4(32010).await.unwrap()
    );
    let mut dremio_client = DremioClient::new(&flight_url, user, pass).await.unwrap();
    let query = "SELECT * FROM sys.options";
    let batches = dremio_client.get_record_batches(query).await.unwrap();
    for batch in batches {
        println!("{:?}", batch);
    }
    let path = "test.parquet";
    dremio_client.write_parquet(query, path).await.unwrap();
    container
        .stop()
        .await
        .expect("Failed to stop Dremio container");
}
