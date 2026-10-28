"""MINILAKE_DUCKDB_MEMORY_LIMIT reaches DuckDB.

The setting used to be parsed and then never passed to any connection, so DuckDB fell back
to its own default (80% of RAM). These tests read the limit back from the engine itself,
because a test of the parsed setting alone is exactly what let that slip through.
"""

from pathlib import Path

import duckdb
import pytest
from databricks.sdk import WorkspaceClient

from minilake.config import settings
from minilake.duckdb_pool import DuckDBPool

MEMORY_LIMIT_SQL = "SELECT current_setting('memory_limit')"


def _reported_limit(limit: str) -> str:
    """How DuckDB displays `limit`, e.g. "4GB" -> "3.7 GiB"."""
    with duckdb.connect(":memory:", config={"memory_limit": limit}) as conn:
        return conn.execute(MEMORY_LIMIT_SQL).fetchone()[0]


@pytest.mark.crud
def test_statement_execution_uses_configured_memory_limit(workspace_client: WorkspaceClient):
    """A query through the Statement Execution API sees the configured limit."""
    wh = workspace_client.warehouses.create(name="memory_limit_wh")
    result = workspace_client.statement_execution.execute_statement(
        warehouse_id=wh.id,
        statement=MEMORY_LIMIT_SQL,
    )

    assert result.result.data_array[0][0] == _reported_limit(settings.duckdb_memory_limit)


@pytest.mark.crud
@pytest.mark.asyncio
async def test_every_pool_connection_gets_the_memory_limit(tmp_path: Path):
    """Both connection paths, the shared UC one and a per-warehouse one, apply the limit."""
    pool = DuckDBPool(tmp_path, memory_limit="16MB")
    try:
        uc = await pool.get_uc_connection()
        wh = await pool.get_warehouse_connection("wh1")

        expected = _reported_limit("16MB")
        assert uc.execute(MEMORY_LIMIT_SQL).fetchone()[0] == expected
        assert wh.execute(MEMORY_LIMIT_SQL).fetchone()[0] == expected
    finally:
        await pool.close_all()


@pytest.mark.error
@pytest.mark.asyncio
async def test_invalid_memory_limit_fails_loudly(tmp_path: Path):
    """A malformed limit raises on connect instead of being silently ignored."""
    pool = DuckDBPool(tmp_path, memory_limit="lots")
    try:
        with pytest.raises(duckdb.Error):
            await pool.get_uc_connection()
    finally:
        await pool.close_all()
