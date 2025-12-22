"""Tests for WSGI compatibility of the TAP server."""

from hats_tap import tap_server


def test_wsgi_app_exports():
    """The tap_server module should expose a WSGI callable for gunicorn."""
    wsgi_app = tap_server.create_app()
    assert wsgi_app is tap_server.app
    assert tap_server.application is tap_server.app
