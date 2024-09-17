use anyhow::Result;
use arrow::array::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::stream::StreamExt;
use parquet::arrow::ArrowWriter;
use tonic::transport::{Channel, Endpoint};

/// A client for interacting with Dremio's Flight SQL service
/// This client is a wrapper around the [`FlightSqlServiceClient`] and provides
/// functionality for returning a [`Vec<RecordBatch>`] from a SQL query
/// as well as writing parquet files from a SQL query
pub struct Client {
    flight_sql_service_client: FlightSqlServiceClient<Channel>,
}

impl Client {
    /// Create a new [`Client`] instance
    /// # Arguments
    /// * `url` - The URL of the Dremio coordinator
    /// * `user` - The username to authenticate with
    /// * `pass` - The password to authenticate with
    /// # Example
    /// ```no_run
    /// use dremio_rs::Client;
    /// #[tokio::main]
    /// async fn main() {
    ///    let mut client = Client::new("http://localhost:32010", "dremio", "dremio123").await.unwrap();
    /// }
    /// ```
    pub async fn new(url: &str, user: &str, pass: &str) -> Result<Self> {
        let mut client =
            FlightSqlServiceClient::new(Endpoint::from_shared(url.to_string())?.connect().await?);
        client.handshake(user, pass).await?;
        Ok(Self {
            flight_sql_service_client: client,
        })
    }

    /// get a [`Vec<RecordBatch>`] from a SQL query
    pub async fn get_record_batches(&mut self, query: &str) -> Result<Vec<RecordBatch>> {
        let flight_info = self
            .flight_sql_service_client
            .execute(query.to_string(), None)
            .await?;
        let ticket = flight_info.endpoint[0]
            .ticket
            .clone()
            .expect("Missing ticket");
        let mut stream = self.flight_sql_service_client.do_get(ticket).await?;
        let mut batches = Vec::new();

        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        Ok(batches)
    }

    /// write a parquet file from a SQL query
    pub async fn write_parquet(&mut self, query: &str, path: &str) -> Result<()> {
        let batches = self.get_record_batches(query).await?;
        let file = std::fs::File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, batches[0].schema(), None)?;
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.close()?;
        Ok(())
    }

    /// Return a reference to the underlying [`FlightSqlServiceClient`]
    pub fn inner(&self) -> &FlightSqlServiceClient<Channel> {
        &self.flight_sql_service_client
    }
}
