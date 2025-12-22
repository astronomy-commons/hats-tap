"""Tests for table metadata endpoints."""

import xml.etree.ElementTree as ET

import pytest

from hats_tap import tap_server
from hats_tap.tap_schema_db import TAPSchemaDatabase


@pytest.fixture
def client_with_metadata(tmp_path, monkeypatch):
    """Configure the TAP server to use a temporary TAP_SCHEMA database."""
    db_path = tmp_path / "tap_schema.db"
    db = TAPSchemaDatabase(str(db_path))
    db.initialize_schema()

    db.insert_schema("gaia_dr3", "Gaia DR3 schema")
    db.insert_table("gaia_dr3", "gaia", "table", "Gaia sources")
    db.insert_table("gaia_dr3", "parents", "table", "Parent table")

    db.insert_column("gaia_dr3.gaia", "source_id", datatype="long", description="Source identifier")
    db.insert_column("gaia_dr3.gaia", "parent_id", datatype="long")
    db.insert_column("gaia_dr3.gaia", "ra", unit="deg", ucd="pos.eq.ra", datatype="double")
    db.insert_column("gaia_dr3.gaia", "dec", unit="deg", ucd="pos.eq.dec", datatype="double")
    db.insert_column("gaia_dr3.parents", "id", datatype="long")

    db.insert_key(
        "fk_parent",
        from_table="gaia_dr3.gaia",
        target_table="gaia_dr3.parents",
        description="Parent relation",
    )
    db.insert_key_column("fk_parent", from_column="parent_id", target_column="id")

    monkeypatch.setattr(tap_server, "tap_schema_db", db)

    app = tap_server.create_app()
    app.config["TESTING"] = True
    return app.test_client()


def _get_table_element(xml_bytes: bytes, table_name: str):
    """Locate a table element by name in a tableset document."""
    ns = {"v": "http://www.ivoa.net/xml/VODataService/v1.1"}
    root = ET.fromstring(xml_bytes)
    for table_elem in root.findall(".//v:table", ns):
        name_elem = table_elem.find("v:name", ns)
        if name_elem is not None and name_elem.text == table_name:
            return table_elem, ns, root
    return None, ns, root


def test_tables_endpoint_includes_columns_and_keys(client_with_metadata):
    """Full tables endpoint should include columns and foreign keys."""
    response = client_with_metadata.get("/tables?detail=max")
    assert response.status_code == 200

    table_elem, ns, _ = _get_table_element(response.data, "gaia")
    assert table_elem is not None

    column_names = {
        name_elem.text
        for col in table_elem.findall("v:column", ns)
        if (name_elem := col.find("v:name", ns)) is not None
    }
    assert {"source_id", "parent_id", "ra", "dec"}.issubset(column_names)

    fk_elems = table_elem.findall("v:foreignKey", ns)
    assert fk_elems
    target_tables = {
        target_elem.text for fk in fk_elems if (target_elem := fk.find("v:targetTable", ns)) is not None
    }
    assert "gaia_dr3.parents" in target_tables

    fk_columns = fk_elems[0].findall("v:fkColumn", ns)
    assert any(
        (from_col := fk_col.find("v:fromColumn", ns)) is not None
        and (target_col := fk_col.find("v:targetColumn", ns)) is not None
        and from_col.text == "parent_id"
        and target_col.text == "id"
        for fk_col in fk_columns
    )


def test_specific_table_endpoint_returns_metadata(client_with_metadata):
    """Table-specific endpoint should return metadata for that table only."""
    response = client_with_metadata.get("/tables/gaia_dr3.gaia")
    assert response.status_code == 200

    table_elem, ns, root = _get_table_element(response.data, "gaia")
    assert table_elem is not None

    tables = root.findall(".//v:table", ns)
    assert len(tables) == 1

    column_names = {
        name_elem.text
        for col in table_elem.findall("v:column", ns)
        if (name_elem := col.find("v:name", ns)) is not None
    }
    assert "source_id" in column_names
