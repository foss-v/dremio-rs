//! Result-set shapes exercised against an in-process Flight SQL server.
//!
//! A real Dremio will not hand back an endpoint with no ticket, or split a
//! small result across several endpoints, on demand — and those are exactly the
//! shapes that used to panic or silently drop rows. Unlike `tests/lib.rs`, none
//! of this needs a Docker daemon.

use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
};
use dremio_rs::{Client, DremioClientError};
use futures::{Stream, StreamExt, stream};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prost::Message;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

/// The shape of the result set the mock server should describe and serve.
#[derive(Clone, Copy)]
enum Shape {
    /// `n` endpoints, each serving one single-row batch holding its own index.
    Endpoints(usize),
    /// A single endpoint that Dremio described without attaching a ticket.
    TicketMissing,
    /// A single endpoint that serves a schema and no rows.
    NoRows,
}

/// The single `Int32` column every mock result set carries.
fn schema() -> Schema {
    Schema::new(vec![Field::new("n", DataType::Int32, false)])
}

/// A one-row batch holding `n`, used to tell endpoints apart.
fn batch(n: i32) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema()),
        vec![Arc::new(Int32Array::from(vec![n]))],
    )
    .expect("batch matches schema")
}

/// A ticket carrying the index of the endpoint it belongs to.
fn ticket_for(index: usize) -> Ticket {
    let query = TicketStatementQuery {
        statement_handle: (index as i32).to_le_bytes().to_vec().into(),
    };
    Ticket {
        ticket: query.as_any().encode_to_vec().into(),
    }
}

#[derive(Clone, Copy)]
struct MockDremio {
    shape: Shape,
}

#[tonic::async_trait]
impl FlightSqlService for MockDremio {
    type FlightService = MockDremio;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Default::default(),
        };
        Ok(Response::new(Box::pin(stream::once(async move {
            Ok(response)
        }))))
    }

    async fn get_flight_info_statement(
        &self,
        _query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // The schema travels on the FlightInfo as well as the stream, which is
        // what lets an endpoint-less result still describe its columns.
        let info = FlightInfo::new()
            .try_with_schema(&schema())
            .map_err(|e| Status::internal(e.to_string()))?;
        let info = match self.shape {
            Shape::Endpoints(n) => (0..n).fold(info, |info, i| {
                info.with_endpoint(FlightEndpoint::new().with_ticket(ticket_for(i)))
            }),
            Shape::TicketMissing => info.with_endpoint(FlightEndpoint::new()),
            Shape::NoRows => info.with_endpoint(FlightEndpoint::new().with_ticket(ticket_for(0))),
        };
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<
        Response<<Self as arrow_flight::flight_service_server::FlightService>::DoGetStream>,
        Status,
    > {
        let batches = match self.shape {
            Shape::NoRows => vec![],
            _ => {
                let handle: [u8; 4] = ticket.statement_handle[..]
                    .try_into()
                    .map_err(|_| Status::invalid_argument("bad statement handle"))?;
                vec![batch(i32::from_le_bytes(handle))]
            }
        };
        // `with_schema` makes the encoder send a schema message even when there
        // are no batches behind it.
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(Arc::new(schema()))
            .build(stream::iter(batches.into_iter().map(Ok)))
            .map(|result| result.map_err(|e| Status::internal(e.to_string())));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Starts a mock server on an ephemeral port and returns its URL. The listener
/// is bound before returning, so a client may connect immediately.
async fn serve(shape: Shape) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("listener has an address");
    tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(MockDremio { shape }))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    format!("http://{addr}")
}

async fn connect(shape: Shape) -> Client {
    let url = serve(shape).await;
    Client::new(&url, "dremio", "dremio123")
        .await
        .expect("mock server accepts the handshake")
}

/// Reads the `n` column out of every batch, in order.
fn values(batches: &[RecordBatch]) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("column is Int32")
                .values()
                .to_vec()
        })
        .collect()
}

/// A path under the temp dir, removed when the guard drops.
struct TempParquet(std::path::PathBuf);

impl TempParquet {
    fn new(name: &str) -> Self {
        Self(std::env::temp_dir().join(format!("dremio-rs-{name}.parquet")))
    }

    fn path(&self) -> &str {
        self.0.to_str().expect("temp path is valid UTF-8")
    }
}

impl Drop for TempParquet {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Reads back a Parquet file as (schema, rows).
fn read_parquet(path: &str) -> (SchemaRef, Vec<RecordBatch>) {
    let file = std::fs::File::open(path).expect("parquet file was written");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("file is valid parquet");
    let schema = builder.schema().clone();
    let batches = builder
        .build()
        .expect("reader builds")
        .collect::<Result<Vec<_>, _>>()
        .expect("batches decode");
    (schema, batches)
}

#[tokio::test]
async fn reads_every_endpoint_not_just_the_first() {
    let mut client = connect(Shape::Endpoints(3)).await;

    let batches = client
        .get_record_batches("SELECT * FROM sys.options")
        .await
        .expect("query succeeds");

    // One row per endpoint, in the order Dremio listed them.
    assert_eq!(values(&batches), vec![0, 1, 2]);
}

#[tokio::test]
async fn endpoint_without_a_ticket_is_an_error_not_a_panic() {
    let mut client = connect(Shape::TicketMissing).await;

    let error = client
        .get_record_batches("SELECT * FROM sys.options")
        .await
        .expect_err("an endpoint with no ticket cannot be read");

    assert!(matches!(error, DremioClientError::MissingTicket));
}

#[tokio::test]
async fn no_endpoints_yields_no_rows() {
    let mut client = connect(Shape::Endpoints(0)).await;

    let batches = client
        .get_record_batches("SELECT * FROM sys.options")
        .await
        .expect("query succeeds");

    assert!(batches.is_empty());
}

#[tokio::test]
async fn empty_result_still_writes_a_parquet_file_with_the_schema() {
    let mut client = connect(Shape::NoRows).await;
    let file = TempParquet::new("empty-result");

    client
        .write_parquet("SELECT * FROM sys.options", file.path())
        .await
        .expect("an empty result set is still writable");

    let (schema, batches) = read_parquet(file.path());
    assert_eq!(schema.field(0).name(), "n");
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
}

#[tokio::test]
async fn empty_result_falls_back_to_the_advertised_schema() {
    // With no endpoints at all there is no stream to take a schema from, so the
    // schema on the FlightInfo is the only thing describing the columns.
    let mut client = connect(Shape::Endpoints(0)).await;
    let file = TempParquet::new("no-endpoints");

    client
        .write_parquet("SELECT * FROM sys.options", file.path())
        .await
        .expect("the advertised schema is enough to write an empty file");

    let (schema, batches) = read_parquet(file.path());
    assert_eq!(schema.field(0).name(), "n");
    assert!(batches.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn every_endpoint_reaches_the_parquet_file() {
    let mut client = connect(Shape::Endpoints(3)).await;
    let file = TempParquet::new("all-endpoints");

    client
        .write_parquet("SELECT * FROM sys.options", file.path())
        .await
        .expect("query succeeds");

    let (_, batches) = read_parquet(file.path());
    assert_eq!(values(&batches), vec![0, 1, 2]);
}
