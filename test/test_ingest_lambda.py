import pytest
from unittest.mock import patch, Mock
from src.ingest_lambda import db_connection

class TestDatabaseConnection:
    '''
    Test suite for the db_connection function.
    '''
    @patch("src.ingest_lambda.pg8000.connect", side_effect=Exception("Connection failed"))
    def test_failed_connection(self, mock_connect):
        '''
        Tests that db_connection raises an error when the database fails to connect.
        '''
        with pytest.raises(Exception, match="Connection failed"):
            db_connection()

    @patch("src.ingest_lambda.pg8000.connect")
    def test_successful_connection(self, mock_connect):
        """
        Tests that db_connection returns the connection object when the connection is successful.
        """
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        result = db_connection()

        assert result == mock_conn

    def test_env_variables(self, monkeypatch):
        '''
        Tests that db_connection correctly reads environment variables.
        '''
        monkeypatch.setenv("USER", "test_user")
        monkeypatch.setenv("PASSWORD", "test_password")
        monkeypatch.setenv("HOST", "test_host")
        monkeypatch.setenv("PORT", "5342")
        monkeypatch.setenv("DATABASE", "test_database")

        with patch("src.ingest_lambda.pg8000.connect") as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value = mock_conn

            result = db_connection()

            mock_connect.assert_called_once_with(
                user = "test_user",
                password = "test_password",
                host = "test_host",
                port = 5342,
                database = "test_database"
            )

        assert result == mock_conn


    