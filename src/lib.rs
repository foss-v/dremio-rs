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
//! # Cargo features
//!
//! Plaintext connections need no features. Reaching a TLS-secured coordinator
//! (`grpc+tls://`, `https://`) needs a crypto provider and a root certificate
//! store, which the `tls` feature enables as a pair:
//!
//! ```toml
//! dremio-rs = { version = "0.2", features = ["tls"] }
//! ```
//!
//! `tls` is shorthand for `tls-ring` and `tls-webpki-roots`. Providers
//! (`tls-ring`, `tls-aws-lc`) and root stores (`tls-webpki-roots`,
//! `tls-native-roots`) can also be picked individually.
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
//!     let batches = client.get_record_batches("SELECT * FROM sys.options").await?;
//!     for batch in batches {
//!         println!("RecordBatch: {:?}", batch);
//!     }
//!
//!     // Write query results to a Parquet file
//!     client.write_parquet("SELECT * FROM sys.options", "sys_options.parquet").await?;
//!
//!     Ok(())
//! }
//! ```

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::stream::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::errors::ParquetError;
use std::fs::File;
use std::io::Error as IoError;
use std::sync::Arc;
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
    /// Dremio described an endpoint holding part of the result set but did not
    /// attach a ticket for retrieving it, so that data cannot be fetched.
    #[error("Flight endpoint has no ticket")]
    MissingTicket,
    /// The query returned no data and Dremio supplied no schema for it, so
    /// there is nothing to describe the columns of an empty Parquet file.
    #[error("Query returned neither data nor a schema")]
    MissingSchema,
}

/// A client for interacting with Dremio's Flight SQL service.
///
/// This client wraps the `FlightSqlServiceClient` and provides a simplified
/// interface for common operations such as executing SQL queries and
/// retrieving data as Arrow `RecordBatch`es, or writing them to Parquet files.
#[derive(Debug)]
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
    /// Dremio may split a result set across several Flight endpoints; every one
    /// of them is read, in order, into the returned vector.
    ///
    /// The whole result set is held in memory. For exports large enough that
    /// this matters, prefer [`Client::write_parquet`], which streams straight to
    /// disk.
    ///
    /// # Arguments
    ///
    /// * `query` - The SQL query string to execute.
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    /// - `Ok(Vec<RecordBatch>)` containing the query results if successful. A
    ///   query that matches no rows yields an empty vector.
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
    ///   let batches = client.get_record_batches("SELECT * FROM sys.options").await.unwrap();
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
        let mut batches = Vec::new();

        for endpoint in flight_info.endpoint {
            let ticket = endpoint.ticket.ok_or(DremioClientError::MissingTicket)?;
            let mut stream = self.flight_sql_service_client.do_get(ticket).await?;
            while let Some(batch) = stream.next().await {
                batches.push(batch?);
            }
        }
        Ok(batches)
    }

    /// Executes a SQL query and writes the results directly to a Parquet file.
    ///
    /// Batches are streamed to disk as they arrive rather than collected first,
    /// so memory use stays flat regardless of how large the result set is.
    ///
    /// A query matching no rows still produces a valid Parquet file carrying the
    /// query's schema and no rows. The file is created only once the query has
    /// succeeded, but a failure part-way through streaming leaves the partial
    /// file behind at `path`.
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
    ///  client.write_parquet("SELECT * FROM sys.options", "my_table.parquet").await.unwrap();
    /// }
    /// ```
    pub async fn write_parquet(
        &mut self,
        query: &str,
        path: &str,
    ) -> Result<(), DremioClientError> {
        let mut flight_info = self
            .flight_sql_service_client
            .execute(query.to_string(), None)
            .await?;
        let endpoints = std::mem::take(&mut flight_info.endpoint);
        // Created from the first batch's schema, so an entirely empty result set
        // leaves this `None` and falls back to the schema Dremio advertised.
        let mut writer: Option<ArrowWriter<File>> = None;
        let mut schema: Option<SchemaRef> = None;

        for endpoint in endpoints {
            let ticket = endpoint.ticket.ok_or(DremioClientError::MissingTicket)?;
            let mut stream = self.flight_sql_service_client.do_get(ticket).await?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                if writer.is_none() {
                    writer = Some(ArrowWriter::try_new(
                        File::create(path)?,
                        batch.schema(),
                        None,
                    )?);
                }
                if let Some(writer) = writer.as_mut() {
                    writer.write(&batch)?;
                }
            }
            if schema.is_none() {
                schema = stream.schema().cloned();
            }
        }

        let writer = match writer {
            Some(writer) => writer,
            None => {
                let schema = match schema {
                    Some(schema) => schema,
                    None => Arc::new(
                        flight_info
                            .try_decode_schema()
                            .map_err(|_| DremioClientError::MissingSchema)?,
                    ),
                };
                ArrowWriter::try_new(File::create(path)?, schema, None)?
            }
        };
        writer.close()?;
        Ok(())
    }

    /// Returns a shared reference to the underlying `FlightSqlServiceClient`.
    ///
    /// Most Flight SQL operations need `&mut self`; use [`Client::inner_mut`] to
    /// call those.
    ///
    /// # Returns
    ///
    /// A reference to the `FlightSqlServiceClient<Channel>`.
    pub fn inner(&self) -> &FlightSqlServiceClient<Channel> {
        &self.flight_sql_service_client
    }

    /// Returns a mutable reference to the underlying `FlightSqlServiceClient`.
    ///
    /// This is the escape hatch for the Flight SQL operations this wrapper does
    /// not expose — prepared statements, catalog metadata, `execute_update` and
    /// so on — all of which take `&mut self`.
    ///
    /// # Returns
    ///
    /// A mutable reference to the `FlightSqlServiceClient<Channel>`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use dremio_rs::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///   let mut client = Client::new("http://localhost:32010", "dremio", "dremio123").await.unwrap();
    ///   let rows = client
    ///     .inner_mut()
    ///     .execute_update("DROP TABLE IF EXISTS scratch.tmp".to_string(), None)
    ///     .await
    ///     .unwrap();
    ///   println!("{rows} rows affected");
    /// }
    /// ```
    pub fn inner_mut(&mut self) -> &mut FlightSqlServiceClient<Channel> {
        &mut self.flight_sql_service_client
    }
}
