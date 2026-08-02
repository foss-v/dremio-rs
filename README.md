# dremio-rs

[![crates.io](https://img.shields.io/crates/v/dremio-rs.svg)](https://crates.io/crates/dremio-rs)
[![docs.rs](https://docs.rs/dremio-rs/badge.svg)](https://docs.rs/dremio-rs)
[![CI](https://github.com/foss-v/dremio-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/foss-v/dremio-rs/actions/workflows/ci.yml)

`dremio-rs` is a Rust client library designed for seamless interaction with Dremio's Flight SQL service. This project serves as both a robust client for Dremio and a learning platform for Rust development, focusing on data-related operations.

## Features

- **Connect to Dremio**: Establish secure connections to your Dremio coordinator using Flight SQL.
- **Execute SQL Queries**: Run SQL queries directly against Dremio and retrieve results.
- **Apache Arrow Integration**: Efficiently handle data with Apache Arrow `RecordBatch`es.
- **Parquet File Export**: Stream query results straight to Parquet files, at constant memory.
- **Asynchronous Operations**: Leverage Rust's `async`/`await` for non-blocking I/O.

## Getting Started

### Prerequisites

- Rust 1.88 or later.
- A running Dremio instance with Flight SQL enabled.

### Installation

Add `dremio-rs` to your `Cargo.toml`:

```toml
[dependencies]
dremio-rs = "0.3"
```

### Usage

Here's a quick example of how to use `dremio-rs` to connect to Dremio, execute a query, and print the results:

```rust
use dremio_rs::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::new("http://localhost:32010", "dremio", "dremio123").await?;

    // Execute a query and get record batches
    let batches = client.get_record_batches("SELECT * FROM sys.options").await?;
    for batch in batches {
        println!("RecordBatch: {:?}", batch);
    }

    // Write query results to a Parquet file
    client.write_parquet("SELECT * FROM sys.options", "sys_options.parquet").await?;

    Ok(())
}
```

For Flight SQL operations this wrapper doesn't expose — prepared statements, catalog
metadata, `execute_update` — reach through to the underlying client with
`Client::inner_mut()`.

### TLS

Plaintext connections work out of the box. Talking to a TLS-secured coordinator
(`grpc+tls://`, `https://`, which includes Dremio Cloud) needs a TLS feature
enabled, since that pulls in a crypto provider and a root certificate store:

```toml
[dependencies]
dremio-rs = { version = "0.3", features = ["tls"] }
```

`tls` is shorthand for `tls-ring` plus `tls-webpki-roots`. To choose your own
combination, enable one provider (`tls-ring` or `tls-aws-lc`) and one root store
(`tls-webpki-roots` or `tls-native-roots`).

## Contributing

Contributions are welcome. Before opening a pull request:

```bash
cargo fmt --all
cargo clippy --all-targets --features tls -- -D warnings
cargo test --lib --test flight_sql   # fast, no Docker needed
cargo test --doc
cargo test --test lib                # integration test, needs Docker
```

The integration test boots `dremio/dremio-oss` with testcontainers. Dremio asks
for a 4 GB heap, so give your Docker VM enough memory or the container is killed
part-way through startup.

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/) —
releases and the changelog are generated from them, so the prefix matters.

## License

This project is licensed under the MIT License - see the `LICENSE-MIT` file for details.
