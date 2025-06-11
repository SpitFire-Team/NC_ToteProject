import pytest
from unittest.mock import patch, Mock, call
from src.load_lambda_package.load_lambda.warehouse_load import (
    wh_connection_engine,
    load_to_warehouse_loop,
)


@pytest.fixture
def mock_db_engine():
    with patch(
        "src.load_lambda_package.load_lambda.warehouse_load.create_engine"
    ) as mock_create_engine:
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        yield mock_engine


class TestConnectionEngine:
    """
    Test suite to test the database connection
    """

    def test_failed_connection(self, mock_db_engine):
        """
        Testing an exception is raised when the connection fails
        """
        mock_db_engine.connect.side_effect = Exception("Connection failed")

        with pytest.raises(Exception):
            wh_connection_engine()

    def test_successful_connection(self, mock_db_engine, monkeypatch):
        """
        Testing that a successful connection can be returned
        """

        monkeypatch.setenv("WH_USER", "test_user")
        monkeypatch.setenv("WH_PASSWORD", "test_password")
        monkeypatch.setenv("WH_HOST", "test_host")
        monkeypatch.setenv("WH_NAME", "test_database")

        mock_conn = Mock(name="pg_connection")
        mock_db_engine.connect.return_value = mock_conn

        result = wh_connection_engine()

        assert result == mock_conn

    def test_connection_string_created_successfully(self, mock_db_engine, monkeypatch):
        """
        Testing that the connection_string is created correctly using the mocked env variables
        """
        monkeypatch.setenv("WH_USER", "test_user")
        monkeypatch.setenv("WH_PASSWORD", "test_password")
        monkeypatch.setenv("WH_HOST", "test_host")
        monkeypatch.setenv("WH_NAME", "test_database")

        mock_conn = Mock(name="pg_connection")
        mock_db_engine.connect.return_value = mock_conn

        wh_connection_engine()

        expected_string = "postgresql://test_user:test_password@test_host/test_database"

        mock_db_engine.mock_calls[0].assert_called_once_with(expected_string)


class TestLoadToWarehouseLoop:
    """
    Test suite to test the load_to_warehouse_loop function
    """

    def test_failed_to_load(self):
        """
        Testing a failed load and correct error message is returned
        """
        mock_df = Mock()

        mock_df.to_sql.side_effect = Exception("DB write failed")

        dict_list = [{"test_table": mock_df}]
        mock_conn = Mock()

        with pytest.raises(
            Exception, match="Could not append to table: DB write failed"
        ):
            load_to_warehouse_loop(dict_list, mock_conn)

    def test_successful_load_single_table(self):
        """
        Testing a loading a single table
        """
        mock_df = Mock()
        mock_df.to_sql = Mock()

        dict_list = [{"test_table": mock_df}]
        mock_conn = Mock()

        load_to_warehouse_loop(dict_list, mock_conn)

        mock_df.to_sql.assert_called_once_with(
            "test_table", con=mock_conn, if_exists="append", index=False
        )

    def test_successful_load_multiple_tables(self):
        """
        Testing a loading multiple tables
        """
        mock_df = Mock()
        mock_df.to_sql = Mock()

        dict_list = [
            {"test_table1": mock_df},
            {"test_table2": mock_df},
            {"test_table3": mock_df},
        ]

        mock_conn = Mock()

        load_to_warehouse_loop(dict_list, mock_conn)

        expected_calls = [
            call("test_table1", con=mock_conn, if_exists="append", index=False),
            call("test_table2", con=mock_conn, if_exists="append", index=False),
            call("test_table3", con=mock_conn, if_exists="append", index=False),
        ]

        assert mock_df.to_sql.call_count == 3
        assert mock_df.to_sql.call_args_list == expected_calls
