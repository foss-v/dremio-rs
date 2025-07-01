# dremio-rs

`dremio-rs` is a Rust client library designed for seamless interaction with Dremio's Flight SQL service. This project serves as both a robust client for Dremio and a learning platform for Rust development, focusing on data-related operations.

## Features

- **Connect to Dremio**: Establish secure connections to your Dremio coordinator using Flight SQL.
- **Execute SQL Queries**: Run SQL queries directly against Dremio and retrieve results.
- **Apache Arrow Integration**: Efficiently handle data with Apache Arrow `RecordBatch`es.
- **Parquet File Export**: Easily export query results to Parquet files.
- **Asynchronous Operations**: Leverage Rust's `async`/`await` for non-blocking I/O.

## Getting Started

### Prerequisites

- Rust (latest stable version recommended)
- A running Dremio instance with Flight SQL enabled.

### Installation

Add `dremio-rs` to your `Cargo.toml`:

```toml
[dependencies]
dremio-rs = "0.1.0" # Use the latest version
```

### Usage

Here's a quick example of how to use `dremio-rs` to connect to Dremio, execute a query, and print the results:

```rust
use dremio_rs::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::new("http://localhost:32010", "dremio", "dremio123").await?;

    // Execute a query and get record batches
    let batches = client.get_record_batches("SELECT * FROM "@dremio"."sys.version"").await?;
    for batch in batches {
        println!("RecordBatch: {:?}", batch);
    }

    // Write query results to a Parquet file
    client.write_parquet("SELECT * FROM "@dremio"."sys.version"", "version.parquet").await?;

    Ok(())
}
```

## Contributing

Contributions are welcome! Please see `CONTRIBUTING.md` (coming soon) for more details.

## License

This project is licensed under the MIT License - see the `LICENSE-MIT` file for details.
