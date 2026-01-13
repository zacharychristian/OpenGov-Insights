import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from usaSpending_project.src.pipelines.load_data_into_postgres import load_data_into_database

# Sample DataFrame for testing
df_sample = pd.DataFrame({
    "id": [1, 2],
    "name": ["Alice", "Bob"],
    "amount": [100, 200]
})

# Success on first try
def test_load_data_success():
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("psycopg2.connect", return_value=mock_conn):
        with patch("psycopg2.extras.execute_values") as mock_execute:
            result = load_data_into_database("test_table", df_sample)

    assert result is True
    mock_execute.assert_called_once()
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()

# Retry once then success
def test_load_data_retry_then_success():
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("psycopg2.connect", return_value=mock_conn):
        with patch("psycopg2.extras.execute_values") as mock_execute:
            # First call raises exception, second call succeeds
            mock_execute.side_effect = [Exception("Temporary failure"), None]
            with patch("time.sleep"):  # avoid actual sleep
                result = load_data_into_database("test_table", df_sample, max_retries=3)

    assert result is True
    assert mock_execute.call_count == 2
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called()

# All retries fail
def test_load_data_all_fail():
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("psycopg2.connect", return_value=mock_conn):
        with patch("psycopg2.extras.execute_values", side_effect=Exception("Database down")):
            with patch("time.sleep"):  # avoid real sleep
                result = load_data_into_database("test_table", df_sample, max_retries=3)

    assert result is False
    mock_conn.commit.assert_not_called()
    mock_cursor.close.assert_called()
