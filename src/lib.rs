//! `dremio-rs` is a Rust client for interacting with Dremio's Flight SQL service.
//!
//! This crate provides a convenient way to connect to a Dremio coordinator,
//! execute SQL queries, and retrieve data as Apache Arrow `RecordBatch`es.
//! It also includes functionality to write query results directly to Parquet files.
//!
//! The client is built on top of the `arrow-flight` and `tonic` crates,
//! offering an asynchronous API for efficient data retrieval.
//!
//! # Features
//!
//! - Connect to Dremio Flight SQL endpoint.
//! - Authenticate with username and password.
//! - Execute SQL queries.
//! - Retrieve query results as `Vec<RecordBatch>`.
//! - Write query results to Parquet files.
//!
//! # Example
//!
//! ```no_run
//! use dremio_rs::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut client = Client::new("http://localhost:32010", "dremio", "dremio123").await?;
//!
//!     // Execute a query and get record batches
//!     let batches = client.get_record_batches("SELECT * FROM "@dremio"."sys.version"").await?;
//!     for batch in batches {
//!         println!("RecordBatch: {:?}", batch);
//!     }
//!
//!     // Write query results to a Parquet file
//!     client.write_parquet("SELECT * FROM "@dremio"."sys.version"", "version.parquet").await?;
//!
//!     Ok(())
//! }
//! ```

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::stream::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::errors::ParquetError;
use std::io::Error as IoError;
use thiserror::Error;
use tonic::transport::{Channel, Endpoint, Error as TonicError};

/// Represents the possible errors that can occur when using the Dremio client.
#[derive(Error, Debug)]
pub enum DremioClientError {
    /// An error originating from the `tonic` gRPC framework.
    #[error("Tonic Error: {0}")]
    TonicError(#[from] TonicError),
    /// An error originating from the `arrow` data processing library.
    #[error("Arrow Error: {0}")]
    ArrowError(#[from] ArrowError),
    /// An error originating from the `arrow-flight` Flight SQL client.
    #[error("Flight Error: {0}")]
    FlightError(#[from] FlightError),
    /// An error originating from standard I/O operations.
    #[error("IO Error: {0}")]
    IoError(#[from] IoError),
    /// An error originating from the `parquet` file format library.
    #[error("Parquet Error: {0}")]
    ParquetError(#[from] ParquetError),
}

/// A client for interacting with Dremio's Flight SQL service.
///
/// This client wraps the `FlightSqlServiceClient` and provides a simplified
/// interface for common operations such as executing SQL queries and
/// retrieving data as Arrow `RecordBatch`es, or writing them to Parquet files.
pub struct Client {
    flight_sql_service_client: FlightSqlServiceClient<Channel>,
}

impl Client {
    /// Creates a new `Client` instance and establishes a connection to the Dremio coordinator.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL of the Dremio coordinator (e.g., "http://localhost:32010").
    /// * `user` - The username for authentication.
    /// * `pass` - The password for authentication.
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    /// - `Ok(Self)` if the connection is successful and authentication succeeds.
    /// - `Err(DremioClientError)` if an error occurs during connection or authentication.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use dremio_rs::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///    let mut client = Client::new("http://localhost:32010", "dremio", "dremio123").await.unwrap();
    /// }
    /// ```
    pub async fn new(url: &str, user: &str, pass: &str) -> Result<Self, DremioClientError> {
        let mut client =
            FlightSqlServiceClient::new(Endpoint::from_shared(url.to_string())?.connect().await?);
        client.handshake(user, pass).await?;
        Ok(Self {
            flight_sql_service_client: client,
        })
    }

    /// Executes a SQL query against Dremio and retrieves the results as a vector of `RecordBatch`es.
    ///
    /// # Arguments
    ///
    /// * `query` - The SQL query string to execute.
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    /// - `Ok(Vec<RecordBatch>)` containing the query results if successful.
    /// - `Err(DremioClientError)` if an error occurs during query execution or data retrieval.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use dremio_rs::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///   let mut client = Client::new("http://localhost:32010", "dremio", "dremio123").await.unwrap();
    ///   let batches = client.get_record_batches("SELECT * FROM "@dremio"."sys.version"").await.unwrap();
    ///   for batch in batches {
    ///     println!("{:?}", batch);
    ///   }
    /// }
    /// ```
    pub async fn get_record_batches(
        &mut self,
        query: &str,
    ) -> Result<Vec<RecordBatch>, DremioClientError> {
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

    /// Executes a SQL query and writes the results directly to a Parquet file.
    ///
    /// # Arguments
    ///
    /// * `query` - The SQL query string to execute.
    /// * `path` - The file path where the Parquet file will be written.
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    /// - `Ok(())` if the Parquet file is successfully written.
    /// - `Err(DremioClientError)` if an error occurs during query execution,
    ///   data retrieval, or file writing.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use dremio_rs::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///  let mut client = Client::new("http://localhost:32010", "dremio", "dremio123").await.unwrap();
    ///  client.write_parquet("SELECT * FROM my_table", "my_table.parquet").await.unwrap();
    /// }
    /// ```
    pub async fn write_parquet(
        &mut self,
        query: &str,
        path: &str,
    ) -> Result<(), DremioClientError> {
        let batches = self.get_record_batches(query).await?;
        let file = std::fs::File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, batches[0].schema(), None)?;
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.close()?;
        Ok(())
    }

    /// Returns a shared reference to the underlying `FlightSqlServiceClient`.
    ///
    /// This can be used to access more advanced Flight SQL operations not directly
    /// exposed by the `Client` interface.
    ///
    /// # Returns
    ///
    /// A reference to the `FlightSqlServiceClient<Channel>`.
    pub fn inner(&self) -> &FlightSqlServiceClient<Channel> {
        &self.flight_sql_service_client
    }
}

