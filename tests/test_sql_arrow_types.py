"""SQL Arrow type and metadata preservation regressions."""

import pytest
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format


@pytest.mark.workflow
def test_external_links_arrow_stream_preserves_sql_types_and_values(workspace_client: WorkspaceClient):
    """Arrow results retain SQL types, binary bytes, and nested values on the wire."""
    from datetime import date, datetime
    from decimal import Decimal

    import pyarrow as pa

    wh = workspace_client.warehouses.create(name="ext_links_arrow_types_wh")
    result = workspace_client.statement_execution.execute_statement(
        warehouse_id=wh.id,
        statement=(
            "SELECT DATE '2024-01-01' AS date_value, "
            "DATE '10000-01-01' AS far_date_value, "
            "DATE 'infinity' AS infinite_date_value, "
            "TIMESTAMP '2024-01-02 03:04:05.123456' AS timestamp_value, "
            "CAST(123.45 AS DECIMAL(7, 2)) AS decimal_value, "
            "from_hex('FF00') AS binary_value, "
            "NULL::INTEGER AS null_value, "
            "[1, 2]::INTEGER[] AS list_value, "
            "struct_pack(label := 'nested') AS struct_value"
        ),
        disposition=Disposition.EXTERNAL_LINKS,
        format=Format.ARROW_STREAM,
    )

    link = result.result.external_links[0]
    response = requests.get(link.external_link)
    assert response.status_code == 200

    table = pa.ipc.open_stream(response.content).read_all()
    assert table.schema.field("date_value").type == pa.date32()
    assert table.schema.field("far_date_value").type == pa.date32()
    assert table.schema.field("infinite_date_value").type == pa.date32()
    assert table.schema.field("timestamp_value").type == pa.timestamp("us")
    assert table.schema.field("decimal_value").type == pa.decimal128(7, 2)
    assert table.schema.field("binary_value").type == pa.binary()
    assert table.schema.field("null_value").type == pa.int32()
    assert table.schema.field("list_value").type == pa.list_(pa.int32())
    assert table.schema.field("struct_value").type == pa.struct([("label", pa.string())])

    assert table.column("date_value").to_pylist() == [date(2024, 1, 1)]
    assert table.column("far_date_value").cast(pa.int32()).to_pylist() == [
        date.max.toordinal() - date(1970, 1, 1).toordinal() + 1
    ]
    assert table.column("infinite_date_value").cast(pa.int32()).to_pylist() == [2**31 - 1]
    assert table.column("timestamp_value").to_pylist() == [datetime(2024, 1, 2, 3, 4, 5, 123456)]
    assert table.column("decimal_value").to_pylist() == [Decimal("123.45")]
    assert table.column("binary_value").to_pylist() == [b"\xff\x00"]
    assert table.column("null_value").to_pylist() == [None]
    assert table.column("list_value").to_pylist() == [[1, 2]]
    assert table.column("struct_value").to_pylist() == [{"label": "nested"}]


def test_arrow_chunk_serialization_preserves_extension_field_metadata():
    """Arrow IPC chunks retain metadata needed to interpret opaque SQL types."""
    import pyarrow as pa

    from minilake.services.sql_statements import _serialize_arrow_table

    field_metadata = {
        b"ARROW:extension:name": b"arrow.opaque",
        b"ARROW:extension:metadata": b'{"type_name":"bignum","vendor_name":"DuckDB"}',
    }
    schema = pa.schema([pa.field("value", pa.binary(), metadata=field_metadata)])
    source = pa.Table.from_arrays([pa.array([b"\x80\x00\x01{"])], schema=schema)

    serialized = _serialize_arrow_table(source, 0, source.num_rows)
    restored = pa.ipc.open_stream(serialized).read_all()

    restored_field = restored.schema.field("value")
    if hasattr(restored_field.type, "extension_name"):
        assert restored_field.type.extension_name == "arrow.opaque"
        assert restored_field.type.storage_type == pa.binary()
        assert restored_field.type.type_name == "bignum"
        assert restored_field.type.vendor_name == "DuckDB"
    else:
        assert restored_field.type == pa.binary()
        assert restored_field.metadata == field_metadata
    assert restored.column("value").to_pylist() == [b"\x80\x00\x01{"]


@pytest.mark.workflow
@pytest.mark.parametrize(
    "statement, expected_rows",
    [
        ("SELECT NULL::INTEGER AS value", [None]),
        ("SELECT NULL::INTEGER AS value WHERE FALSE", []),
    ],
    ids=["typed-all-null", "typed-zero-rows"],
)
def test_external_links_arrow_stream_preserves_typed_null_and_empty_columns(
    workspace_client: WorkspaceClient, statement: str, expected_rows: list
):
    """Null-only and empty Arrow results retain the declared SQL column type."""
    import pyarrow as pa

    wh = workspace_client.warehouses.create(name="ext_links_arrow_empty_wh")
    result = workspace_client.statement_execution.execute_statement(
        warehouse_id=wh.id,
        statement=statement,
        disposition=Disposition.EXTERNAL_LINKS,
        format=Format.ARROW_STREAM,
    )

    link = result.result.external_links[0]
    response = requests.get(link.external_link)
    assert response.status_code == 200

    table = pa.ipc.open_stream(response.content).read_all()
    assert table.schema.field("value").type == pa.int32()
    assert table.column("value").to_pylist() == expected_rows
