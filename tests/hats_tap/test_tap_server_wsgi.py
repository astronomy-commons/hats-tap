"""Tests for WSGI compatibility of the TAP server."""

from hats_tap import tap_server


def test_wsgi_app_exports():
    """The tap_server module should expose a WSGI callable for gunicorn."""
    wsgi_app = tap_server.create_app()
    assert wsgi_app is tap_server.app
    assert tap_server.application is tap_server.app


def test_sync_query_computes_count_distinct_as_unique_values():
    """Return the issue-directed unique values through the TAP endpoint."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    catalog = MagicMock()
    column = catalog.__getitem__.return_value
    column.unique.return_value.compute.return_value = pd.Series([42, 7], name="diaObjectId")

    with (
        patch.object(tap_server.lsdb, "open_catalog", return_value=catalog) as open_catalog,
        patch.object(tap_server, "get_column_metadata", return_value={}),
    ):
        response = tap_server.app.test_client().post(
            "/sync",
            data={
                "REQUEST": "doQuery",
                "LANG": "ADQL",
                "QUERY": "SELECT COUNT(DISTINCT diaObjectId) FROM ppdb.DiaObject",
            },
        )

    assert response.status_code == 200
    open_catalog.assert_called_once_with(
        "/var/www/data.lsdb.io/html/hats/ppdb/DiaObject/",
        columns=["diaObjectId"],
        search_filter=None,
        filters=[],
    )
    catalog.__getitem__.assert_called_once_with("diaObjectId")
    catalog.head.assert_not_called()
    assert b'<FIELD name="diaObjectId"' in response.data
    assert b"<TD>42</TD>" in response.data
    assert b"<TD>7</TD>" in response.data
