<p align="center">
  <img src="https://raw.githubusercontent.com/dmux/minilake/main/minilake_logo.png" alt="minilake" width="240"/>
</p>

<h1 align="center">MiniLake</h1>

<p align="center">
  <a href="https://github.com/dmux/minilake/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/dmux/minilake/ci.yml?branch=main&label=CI" alt="CI"></a>
  <a href="https://github.com/dmux/minilake/releases"><img src="https://img.shields.io/github/v/release/dmux/minilake" alt="Release"></a>
  <a href="https://github.com/dmux/minilake/blob/main/LICENSE"><img src="https://img.shields.io/github/license/dmux/minilake" alt="License"></a>
</p>

MiniLake is a local emulator of the Databricks REST API, for developing and testing
`databricks-sdk`, CLI and Terraform code without a workspace.

SQL statements run on DuckDB. EXTERNAL Delta tables are Parquet files with a `_delta_log/`,
and writes to them go through a generated Spark job. Jobs run `spark-submit` in a sibling
Docker container. Everything else in the API is either stored and read back, or answers
`501 NOT_IMPLEMENTED`. It is meant for one developer on localhost, not as a shared server.

Website: <https://dmux.github.io/minilake/>

## Run it

```bash
pip install minilake
minilake --port 8000

# or
docker run -p 8000:8000 ghcr.io/dmux/minilake:latest

curl http://localhost:8000/_minilake/health
```

The web SQL editor is at <http://localhost:8000/ui/>. Any token is accepted. The image
downloads nothing at runtime, so it also works offline
([details](https://github.com/dmux/minilake/blob/main/docs/getting-started.md#offline-use)).

## Use it

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(host="http://localhost:8000", token="dev")
w.catalogs.create(name="sales")
w.schemas.create(name="store", catalog_name="sales")
wh = w.warehouses.create(name="dev", cluster_size="Small")

def sql(statement):
    return w.statement_execution.execute_statement(warehouse_id=wh.id, statement=statement)

sql("CREATE TABLE sales.store.orders (id BIGINT, customer VARCHAR)")
sql("INSERT INTO sales.store.orders VALUES (1, 'Ana')")
print(sql("SELECT * FROM sales.store.orders").result.data_array)  # [[1, 'Ana']]
```

## Documentation

- [Getting started](https://github.com/dmux/minilake/blob/main/docs/getting-started.md)
- [Configuration](https://github.com/dmux/minilake/blob/main/docs/configuration.md): environment variables, persistence, TLS
- [Databricks SDK](https://github.com/dmux/minilake/blob/main/docs/databricks-sdk.md)
- [Terraform and Asset Bundles](https://github.com/dmux/minilake/blob/main/docs/terraform.md)
- [Spark and Delta Lake](https://github.com/dmux/minilake/blob/main/docs/spark-and-delta.md)
- [Web UI](https://github.com/dmux/minilake/blob/main/docs/ui.md)
- [MCP server](https://github.com/dmux/minilake/blob/main/docs/mcp/index.md): optional, `MINILAKE_MCP=1`, for LLM agents
- [Testing and development](https://github.com/dmux/minilake/blob/main/docs/testing.md)
- [FEATURES.md](https://github.com/dmux/minilake/blob/main/FEATURES.md): status of every service, with design notes
- [API coverage](https://github.com/dmux/minilake/blob/main/docs/CLI_COVERAGE.md): every SDK endpoint probed through the `databricks` CLI
- [Emulation roadmap](https://github.com/dmux/minilake/blob/main/docs/EMULATION_ROADMAP.md)

## Known Gaps

These are deliberate. MiniLake targets one developer running it locally.

- **No authentication.** Any token is accepted. SCIM users and personal access tokens are
  records, not credentials.
- **No access control.** The Permissions and Grants APIs store and return what you give
  them, and nothing is enforced. A test that passes here says nothing about grants in a
  real workspace.
- **The SQL dialect is DuckDB's**, not Spark SQL.
- **Clusters are a state machine.** There is no Spark on them; Spark runs through Jobs.
- **MANAGED tables are DuckDB tables with no files**, so Spark cannot see them. Use EXTERNAL
  Delta tables when Spark has to read the data.
- **Single process.** DuckDB is single-writer, so concurrent load serializes on a lock.
- **Not implemented, returns 501:** Repos, DLT, Model Registry, Vector Search, Lakeview and
  Genie, Delta Sharing, and non-Python notebook languages.
- **Uneven test coverage.** `jobs.py`, `sql_statements.py` and `unity_catalog.py` are
  tested mostly on the happy path.

MiniLake does not reproduce every quirk of the real service. Tests here run against the
real `databricks-sdk`, and the Terraform provider and `databricks bundle` are exercised
end to end, but a green run locally is not a substitute for a run against Databricks.

## Contributing

See [CONTRIBUTING.md](https://github.com/dmux/minilake/blob/main/CONTRIBUTING.md).

## License

MIT. See [LICENSE](https://github.com/dmux/minilake/blob/main/LICENSE).
