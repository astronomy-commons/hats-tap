"""Tests for thread-safety of TAPSchemaDatabase."""

import sqlite3
import threading

import pytest

from hats_tap.tap_schema_db import TAPSchemaDatabase


class TestThreadSafety:
    """Test that TAPSchemaDatabase is thread-safe."""

    @pytest.fixture
    def temp_db(self, tmp_path):
        """Create a temporary database for testing."""
        db_path = tmp_path / "test_tap_schema.db"
        db = TAPSchemaDatabase(str(db_path))
        db.initialize_schema()
        # Insert some test data
        db.insert_schema("test_schema", "Test schema")
        db.insert_table("test_schema", "test_table", "table", "Test table")
        return db

    def test_multiple_threads_can_query(self, temp_db):
        """Test that multiple threads can query the database concurrently."""
        results = []
        errors = []

        def query_in_thread():
            """Query the database in a thread."""
            try:
                schemas = temp_db.query("SELECT schema_name FROM schemas")
                results.append(schemas)
            except Exception as e:
                errors.append(e)

        # Create multiple threads
        threads = [threading.Thread(target=query_in_thread) for _ in range(10)]

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check that all threads succeeded
        assert len(errors) == 0, f"Expected no errors, but got: {errors}"
        assert len(results) == 10, f"Expected 10 results, but got {len(results)}"

        # Check that each result contains the test schema
        for result in results:
            assert len(result) == 1
            assert result[0]["schema_name"] == "test_schema"

    def test_each_thread_has_own_connection(self, temp_db):
        """Test that each thread has its own connection."""
        connection_ids = []

        def get_connection_id():
            """Get the connection ID in a thread."""
            temp_db.connect()
            # Get the connection object id (not thread-safe, but useful for testing)
            connection_ids.append(id(temp_db.connection))

        # Create multiple threads
        threads = [threading.Thread(target=get_connection_id) for _ in range(5)]

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check that each thread had a different connection
        assert len(connection_ids) == 5
        assert len(set(connection_ids)) == 5, "Each thread should have its own connection"

    def test_context_manager_works_with_threads(self, temp_db):
        """Test that context manager works correctly with threads."""
        results = []
        errors = []

        def query_with_context_manager():
            """Query using context manager in a thread."""
            try:
                with temp_db as db:
                    schemas = db.query("SELECT schema_name FROM schemas")
                    results.append(schemas)
            except Exception as e:
                errors.append(e)

        # Create multiple threads
        threads = [threading.Thread(target=query_with_context_manager) for _ in range(10)]

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check that all threads succeeded
        assert len(errors) == 0, f"Expected no errors, but got: {errors}"
        assert len(results) == 10

    def test_no_thread_safety_error(self, temp_db):
        """Test that we don't get the SQLite thread safety error."""
        errors = []

        def query_repeatedly():
            """Query the database repeatedly in a thread."""
            try:
                for _ in range(10):
                    temp_db.query("SELECT schema_name FROM schemas")
            except sqlite3.ProgrammingError as e:
                if "thread" in str(e).lower():
                    errors.append(e)

        # Create multiple threads
        threads = [threading.Thread(target=query_repeatedly) for _ in range(5)]

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check that we didn't get any thread safety errors
        assert len(errors) == 0, f"Got thread safety errors: {errors}"
