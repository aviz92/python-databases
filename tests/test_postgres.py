# test_postgresql.py

import pytest
from unittest.mock import patch, MagicMock
from python_databases.postgresql_infrastructure import PostgreSQL


@pytest.fixture
def mock_connection():
    with patch("psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        yield mock_conn, mock_cursor


def test_connection_created(mock_connection):
    conn, _ = mock_connection
    db = PostgreSQL(name="test_db", host="localhost", port="5432", username="user", password="pass")
    assert db.client == conn
    assert conn.cursor.call_count == 2


def test_execute_query_success(mock_connection):
    _, cursor = mock_connection
    db = PostgreSQL(name="test_db", host="localhost", port="5432", username="user", password="pass")
    db.execute_query("SELECT 1", None)
    cursor.execute.assert_called_with("SELECT 1", None)


def test_fetch_all_success(mock_connection):
    _, cursor = mock_connection
    cursor.fetchall.return_value = [{"id": 1}]
    db = PostgreSQL(name="test_db", host="localhost", port="5432", username="user", password="pass")
    results = db.fetch_all("SELECT id FROM test")
    cursor.execute.assert_called_with("SELECT id FROM test", None)
    assert results == [{"id": 1}]


def test_context_manager_closes_connection(mock_connection):
    conn, cursor = mock_connection
    with PostgreSQL(name="test_db", host="localhost", port="5432", username="user", password="pass") as db:
        pass
    cursor.close.assert_called_once()
    conn.close.assert_called_once()
