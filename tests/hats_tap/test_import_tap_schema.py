"""Tests for TAP schema import functionality."""

import pytest

from hats_tap.tap_schema_db import TAPSchemaDatabase


class MockTAPService:
    """Mock TAP service for testing."""

    def __init__(self):
        self.responses = {}

    def add_response(self, query_pattern, rows, fieldnames):
        """Add a mock response for a query pattern."""
        self.responses[query_pattern] = {"rows": rows, "fieldnames": fieldnames}

    def search(self, query):
        """Mock search method."""
        # Match the query to a pattern
        for pattern, response in self.responses.items():
            if pattern in query:
                return MockResult(response["rows"], response["fieldnames"])
        return MockResult([], [])


class MockResult:
    """Mock result from TAP query."""

    def __init__(self, rows, fieldnames):
        self.rows = rows
        self.fieldnames = fieldnames

    def __iter__(self):
        return iter(self.rows)


class MockRow:
    """Mock row from TAP result."""

    def __init__(self, data):
        self.data = data

    def __getitem__(self, key):
        return self.data.get(key)


def test_import_table_with_schema_qualified_local_name(tmp_path, monkeypatch):
    """Test importing a table with a schema-qualified local name like 'gaia_dr3.gaia'."""
    from hats_tap.import_tap_schema import TAPSchemaImporter

    # Setup database
    db_path = tmp_path / "test_tap_schema.db"
    db = TAPSchemaDatabase(str(db_path))
    db.initialize_schema()

    # Create mock TAP service
    mock_service = MockTAPService()

    # Mock response for table lookup
    mock_service.add_response(
        "FROM TAP_SCHEMA.tables",
        [
            MockRow(
                {
                    "schema_name": "gaia",
                    "table_name": "gaia_dr3_source",
                    "table_type": "table",
                    "description": "Gaia DR3 source catalog",
                    "utype": None,
                }
            )
        ],
        ["schema_name", "table_name", "table_type", "description", "utype"],
    )

    # Mock response for schema lookup (for both 'gaia' and 'gaia_dr3')
    mock_service.add_response(
        "FROM TAP_SCHEMA.schemas",
        [],  # Schema doesn't exist on remote server
        ["schema_name", "description", "utype"],
    )

    # Mock response for columns
    mock_service.add_response(
        "FROM TAP_SCHEMA.columns",
        [
            MockRow(
                {
                    "table_name": "gaia_dr3_source",
                    "column_name": "source_id",
                    "datatype": "BIGINT",
                    "description": "Source identifier",
                    "unit": None,
                    "ucd": "meta.id;meta.main",
                    "utype": None,
                    "size": None,
                    "principal": 1,
                    "indexed": 1,
                    "std": 0,
                }
            ),
            MockRow(
                {
                    "table_name": "gaia_dr3_source",
                    "column_name": "ra",
                    "datatype": "DOUBLE",
                    "description": "Right ascension",
                    "unit": "deg",
                    "ucd": "pos.eq.ra;meta.main",
                    "utype": None,
                    "size": None,
                    "principal": 1,
                    "indexed": 0,
                    "std": 0,
                }
            ),
        ],
        [
            "table_name",
            "column_name",
            "datatype",
            "description",
            "unit",
            "ucd",
            "utype",
            "size",
            "principal",
            "indexed",
            "std",
        ],
    )

    # Mock response for keys
    mock_service.add_response(
        "FROM TAP_SCHEMA.keys",
        [],
        ["key_id", "from_table", "target_table", "description", "utype"],
    )

    # Create importer
    importer = TAPSchemaImporter("http://mock.tap.service/TAP", str(db_path))

    # Replace service with mock
    importer.service = mock_service
    importer.db = db
    importer.db.connect()

    # Import table with schema-qualified local name
    success = importer.import_table_by_name(
        table_name="gaia_dr3_source",
        include_keys=True,
        local_table_name="gaia_dr3.gaia",
    )

    assert success

    # Verify schema was created
    schemas = db.query("SELECT * FROM schemas WHERE schema_name = 'gaia_dr3'")
    assert len(schemas) == 1
    assert schemas[0]["schema_name"] == "gaia_dr3"

    # Verify table was inserted with correct schema and table name
    tables = db.query("SELECT * FROM tables WHERE schema_name = 'gaia_dr3'")
    assert len(tables) == 1
    assert tables[0]["schema_name"] == "gaia_dr3"
    assert tables[0]["table_name"] == "gaia"  # Should be just 'gaia', not 'gaia_dr3.gaia'

    # Verify columns were inserted with fully qualified table name
    columns = db.query("SELECT * FROM columns WHERE table_name = 'gaia_dr3.gaia'")
    assert len(columns) == 2
    column_names = {col["column_name"] for col in columns}
    assert "source_id" in column_names
    assert "ra" in column_names


def test_import_table_with_simple_local_name(tmp_path, monkeypatch):
    """Test importing a table with a simple local name (no schema qualification)."""
    from hats_tap.import_tap_schema import TAPSchemaImporter

    # Setup database
    db_path = tmp_path / "test_tap_schema.db"
    db = TAPSchemaDatabase(str(db_path))
    db.initialize_schema()

    # Create mock TAP service
    mock_service = MockTAPService()

    # Mock response for table lookup
    mock_service.add_response(
        "FROM TAP_SCHEMA.tables",
        [
            MockRow(
                {
                    "schema_name": "gaia",
                    "table_name": "gaia_dr3_source",
                    "table_type": "table",
                    "description": "Gaia DR3 source catalog",
                    "utype": None,
                }
            )
        ],
        ["schema_name", "table_name", "table_type", "description", "utype"],
    )

    # Mock response for schema lookup
    mock_service.add_response(
        "FROM TAP_SCHEMA.schemas",
        [MockRow({"schema_name": "gaia", "description": "Gaia schema", "utype": None})],
        ["schema_name", "description", "utype"],
    )

    # Mock response for columns
    mock_service.add_response(
        "FROM TAP_SCHEMA.columns",
        [
            MockRow(
                {
                    "table_name": "gaia_dr3_source",
                    "column_name": "source_id",
                    "datatype": "BIGINT",
                    "description": "Source identifier",
                    "unit": None,
                    "ucd": "meta.id;meta.main",
                    "utype": None,
                    "size": None,
                    "principal": 1,
                    "indexed": 1,
                    "std": 0,
                }
            )
        ],
        [
            "table_name",
            "column_name",
            "datatype",
            "description",
            "unit",
            "ucd",
            "utype",
            "size",
            "principal",
            "indexed",
            "std",
        ],
    )

    # Mock response for keys
    mock_service.add_response(
        "FROM TAP_SCHEMA.keys",
        [],
        ["key_id", "from_table", "target_table", "description", "utype"],
    )

    # Create importer
    importer = TAPSchemaImporter("http://mock.tap.service/TAP", str(db_path))

    # Replace service with mock
    importer.service = mock_service
    importer.db = db
    importer.db.connect()

    # Import table with simple local name
    success = importer.import_table_by_name(
        table_name="gaia_dr3_source",
        include_keys=True,
        local_table_name="my_gaia_table",
    )

    assert success

    # Verify table was inserted with remote schema and custom table name
    tables = db.query("SELECT * FROM tables WHERE schema_name = 'gaia'")
    assert len(tables) == 1
    assert tables[0]["schema_name"] == "gaia"
    assert tables[0]["table_name"] == "my_gaia_table"

    # Verify columns were inserted with fully qualified table name
    columns = db.query("SELECT * FROM columns WHERE table_name = 'gaia.my_gaia_table'")
    assert len(columns) == 1
    assert columns[0]["column_name"] == "source_id"


def test_import_table_without_local_name(tmp_path, monkeypatch):
    """Test importing a table without specifying a local name."""
    from hats_tap.import_tap_schema import TAPSchemaImporter

    # Setup database
    db_path = tmp_path / "test_tap_schema.db"
    db = TAPSchemaDatabase(str(db_path))
    db.initialize_schema()

    # Create mock TAP service
    mock_service = MockTAPService()

    # Mock response for table lookup
    mock_service.add_response(
        "FROM TAP_SCHEMA.tables",
        [
            MockRow(
                {
                    "schema_name": "gaia",
                    "table_name": "gaia_dr3_source",
                    "table_type": "table",
                    "description": "Gaia DR3 source catalog",
                    "utype": None,
                }
            )
        ],
        ["schema_name", "table_name", "table_type", "description", "utype"],
    )

    # Mock response for schema lookup
    mock_service.add_response(
        "FROM TAP_SCHEMA.schemas",
        [MockRow({"schema_name": "gaia", "description": "Gaia schema", "utype": None})],
        ["schema_name", "description", "utype"],
    )

    # Mock response for columns
    mock_service.add_response(
        "FROM TAP_SCHEMA.columns",
        [
            MockRow(
                {
                    "table_name": "gaia_dr3_source",
                    "column_name": "source_id",
                    "datatype": "BIGINT",
                    "description": "Source identifier",
                    "unit": None,
                    "ucd": "meta.id;meta.main",
                    "utype": None,
                    "size": None,
                    "principal": 1,
                    "indexed": 1,
                    "std": 0,
                }
            )
        ],
        [
            "table_name",
            "column_name",
            "datatype",
            "description",
            "unit",
            "ucd",
            "utype",
            "size",
            "principal",
            "indexed",
            "std",
        ],
    )

    # Mock response for keys
    mock_service.add_response(
        "FROM TAP_SCHEMA.keys",
        [],
        ["key_id", "from_table", "target_table", "description", "utype"],
    )

    # Create importer
    importer = TAPSchemaImporter("http://mock.tap.service/TAP", str(db_path))

    # Replace service with mock
    importer.service = mock_service
    importer.db = db
    importer.db.connect()

    # Import table without local name
    success = importer.import_table_by_name(
        table_name="gaia_dr3_source",
        include_keys=True,
        local_table_name=None,
    )

    assert success

    # Verify table was inserted with original schema and table name
    tables = db.query("SELECT * FROM tables WHERE schema_name = 'gaia'")
    assert len(tables) == 1
    assert tables[0]["schema_name"] == "gaia"
    assert tables[0]["table_name"] == "gaia_dr3_source"

    # Verify columns were inserted with fully qualified table name
    columns = db.query("SELECT * FROM columns WHERE table_name = 'gaia.gaia_dr3_source'")
    assert len(columns) == 1
    assert columns[0]["column_name"] == "source_id"
