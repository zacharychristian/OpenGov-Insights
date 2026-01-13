from unittest.mock import MagicMock, patch
import pytest
from usaSpending_project.src.pipelines.get_financial_data import (
    get_financial_data_from_api,
)
import requests
import time


# Test for success on first try
def test_get_financial_data_success():
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"results": [{"agency_id": 1, "amount": 1000}]}

    with patch("requests.get", return_value=mock_response):
        with patch("time.sleep"):
            result = get_financial_data_from_api(2024, agency_id=1)

    assert result == [{"agency_id": 1, "amount": 1000}]


# Test for retry once then success
def test_get_financial_data_retry_then_success():
    # First call: fails with RequestException
    mock_fail = MagicMock()
    mock_fail.raise_for_status.side_effect = requests.exceptions.RequestException("Temporary failure")

    # Second call: succeeds
    mock_success = MagicMock()
    mock_success.raise_for_status.return_value = None
    mock_success.json.return_value = {"results": [{"agency_id": 2, "amount": 500}]}

    with patch("requests.get", side_effect=[mock_fail, mock_success]):
        with patch("time.sleep"):
            result = get_financial_data_from_api(2024, agency_id=2, max_retries=3)

    assert result == [{"agency_id": 2, "amount": 500}]


# Test for all retries failed. List should be empty
def test_get_financial_data_all_fail_returns_empty():
    mock_fail = MagicMock()
    mock_fail.raise_for_status.side_effect = requests.exceptions.RequestException("API down")

    with patch("requests.get", return_value=mock_fail):
        with patch("time.sleep"):
            result = get_financial_data_from_api(2024, agency_id=3, max_retries=3)

    assert result == []

# Testing for when the response is empty. Should return [] and not throw an exception.
def test_get_financial_data_empty_results():
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"results": []}

    with patch("requests.get", return_value=mock_response):
        with patch("time.sleep"):
            result = get_financial_data_from_api(2024, agency_id=4)

    assert result == []
