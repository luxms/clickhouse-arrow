# ClickHouse Arrow client for Rust

An asynchronous Rust client for reading and writing ClickHouse data as Apache
Arrow record batches over ClickHouse's **native TCP protocol**.

The goal is to connect ClickHouse to Arrow-based applications without requiring
an HTTP/JSON transport or row-by-row application conversion. The client provides
streaming queries and inserts, SQL execution, table creation from schemas,
compression, and connection-pooling options. It also supports typed Rust rows
through a derive macro.

This is the Luxms-maintained fork of
[GeorgeLeePatterson/clickhouse-arrow](https://github.com/GeorgeLeePatterson/clickhouse-arrow),
used by Kuboring and the [Luxms ADBC driver](https://github.com/luxms/adbc-clickhouse).
The fork uses **Arrow 59** and **Rust 1.96.0**. Use the Git dependency below to
get this fork; a crates.io dependency does not select the Luxms changes.

## Native client or ADBC driver?

```text
Async Rust application ──────────────────────→ clickhouse-arrow → ClickHouse
Application using ADBC → adbc-clickhouse ────→ clickhouse-arrow → ClickHouse
```

Use **this client** when you want the async, ClickHouse-specific API, including
native-client configuration and schema-based table creation.

Use **[adbc-clickhouse](https://github.com/luxms/adbc-clickhouse)** when your
application needs Arrow Database Connectivity's standard driver/database/
connection/statement interfaces. That project wraps this client with a
synchronous ADBC API; it does not implement a second network protocol.

The dependency goes one way: ADBC depends on this client, not the reverse.
Both exchange Arrow values in-process, so their Arrow major versions must match.
If you depend directly on both, use the client revision pinned by the ADBC
driver's [Cargo.toml](https://github.com/luxms/adbc-clickhouse/blob/main/Cargo.toml).

## Quick start

You need a ClickHouse server with its **native TCP endpoint** available,
normally `127.0.0.1:9000`. This is not the HTTP endpoint on port `8123`.

Add the following to a Rust application's `Cargo.toml`. The revision is a
tested commit merged into Luxms `main`; the package version remains `0.2.1`.

```toml
[dependencies]
clickhouse-arrow = { git = "https://github.com/luxms/clickhouse-arrow", rev = "973c3ccc03bcaae686024d29cc368fac21424173" }
futures-util = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Put this in `src/main.rs`. It reads three generated rows and does not modify
any tables:

```rust
use clickhouse_arrow::{
    ClientBuilder,
    arrow::arrow::array::UInt64Array,
};
use futures_util::TryStreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("CLICKHOUSE_ENDPOINT")
        .unwrap_or_else(|_| "127.0.0.1:9000".into());
    let username = std::env::var("CLICKHOUSE_USER")
        .unwrap_or_else(|_| "default".into());
    let password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();

    let client = ClientBuilder::new()
        .with_endpoint(endpoint)
        .with_database("default")
        .with_username(username)
        .with_password(password)
        .build_arrow()
        .await?;

    let mut rows = client.query("SELECT number AS id FROM numbers(3)", None).await?;
    while let Some(batch) = rows.try_next().await? {
        let ids = batch.column(0).as_any().downcast_ref::<UInt64Array>()
            .ok_or("expected a UInt64 id column")?;
        for id in ids.values() {
            println!("{id}");
        }
    }
    Ok(())
}
```

Set `CLICKHOUSE_ENDPOINT`, `CLICKHOUSE_USER`, and `CLICKHOUSE_PASSWORD` in your
environment as needed, then run `cargo run`. Expected output is `0`, `1`, and
`2`, each on its own line. These variables are read by the example, not
automatically by the client.

This example uses an unencrypted native connection. For TLS, configure
`ClientBuilder::with_tls(true)` and the server's TLS-enabled native endpoint;
do not substitute an HTTPS URL for a TCP address. See the
[builder API](clickhouse-arrow/src/client/builder.rs) for available settings.

## Working with data

- **Query Arrow batches:** `build_arrow()` creates an Arrow client; `query(sql, None)`
  returns a stream. Consume it with `try_next` or `try_collect` to receive
  both results and errors that arrive after the initial request.
- **Insert Arrow batches:** `insert(sql, batch, None)` returns a completion
  stream. Consume that stream before reporting success, for example with
  `try_for_each(|()| async { Ok(()) })`. Dropping it can lose completion errors.
- **Execute SQL:** use `execute(sql, None)` for statements without result rows.
- **Create tables from schemas:** use `create_table` with `CreateOptions`,
  including `with_cluster("cluster_name")` for `ON CLUSTER` creation.
- **Work with typed Rust rows:** use `build_native()` and the `Row` derive
  macro. Arrow remains a dependency even when using the native-row API.

The `None` argument in these examples lets the client choose a query ID.
See the [examples](clickhouse-arrow/examples/) and
[integration tests](clickhouse-arrow/tests/) for larger workflows. The
[crate-level guide](clickhouse-arrow/README.md) provides type mappings and
conversion caveats; some upstream examples and benchmark results are historical.

Type conversion is not lossless for every ClickHouse/Arrow combination.
Consult that guide and test the types used by your workload rather than assuming
every server type or feature is supported.

## Crates and features

- [`clickhouse-arrow`](clickhouse-arrow/) contains the client, Arrow conversion,
  and native protocol implementation.
- [`clickhouse-arrow-derive`](clickhouse-arrow-derive/) provides the `Row` macro.

Default features are `derive`, `serde`, `pool` (bb8 pooling), and `inner_pool`
(internal TCP connection pooling). Optional features include `extended-types`,
`geo-types`, `rust_decimal`, and `cloud`. See the
[crate manifest](clickhouse-arrow/Cargo.toml) for their definitions.
Enabling a feature is not a guarantee that every server configuration is supported.

Luxms compatibility work includes fallible Arrow 59 union construction, rejecting
nullable map keys before Arrow can panic, explicitly negotiating LZ4 when selected,
and preserving `ON CLUSTER` table creation used by Kuboring.

## Development and validation

Use the pinned **Rust 1.96.0** toolchain for compilation, formatting, linting,
tests, and coverage. Nightly is not required.

With Docker running, install the task runner and coverage tooling if needed:

```sh
cargo install just cargo-llvm-cov --locked
rustup component add llvm-tools-preview --toolchain 1.96.0
just checks
just check-features
```

`just checks` is the required local gate: formatting, strict lint, default and
extended-type unit/integration suites, and greater than 90% line coverage.
`just check-features` runs the feature-specific compile/lint matrix.

Tests start disposable ClickHouse containers. If the image is already cached,
set `CLICKHOUSE_PULL_LATEST=false` to avoid pulling it again. Use
`RUST_TEST_THREADS=1` to reduce concurrent container startup when Docker is
resource-constrained. Coverage already runs serially.

For performance measurements, run the [benchmarks](clickhouse-arrow/benches/)
against your own workload; historical upstream numbers are not a guarantee for
this fork or your deployment.

## License and upstream

Apache-2.0; see [LICENSE](LICENSE). See [CONTRIBUTING.md](CONTRIBUTING.md) for
upstream contribution guidance. This fork retains the original project's
attribution, including its acknowledgment of [klickhouse](https://github.com/Protryon/klickhouse)
as an early source of inspiration and code.
