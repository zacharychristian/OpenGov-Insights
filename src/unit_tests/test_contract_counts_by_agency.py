import requests
from unittest.mock import MagicMock, patch
from usaSpending_project.src.pipelines.get_contract_counts_by_agency import (
    get_contract_counts_by_year_from_api,
)
import pytest
import requests
import time

# Test for success on first try
def test_get_contract_counts_by_year_success():
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"results": [{"agency": "DOD", "count": 1234}]}

    with patch("requests.get", return_value=mock_response):
        with patch("time.sleep"):
            result = get_contract_counts_by_year_from_api(2024)

    assert result == {"agency": "DOD", "count": 1234}


# Test for one failure, then success
def test_get_contract_counts_by_year_retry_then_success():
    # First call: fails with RequestException
    mock_fail = MagicMock()
    mock_fail.raise_for_status.side_effect = requests.exceptions.RequestException("Temporary failure")

    # Second call: success
    mock_success = MagicMock()
    mock_success.raise_for_status.return_value = None
    mock_success.json.return_value = {"results": [{"agency": "NASA", "count": 999}]}

    with patch("requests.get", side_effect=[mock_fail, mock_success]):
        with patch("time.sleep"):
            result = get_contract_counts_by_year_from_api(2024, max_retries=3)

    assert result == {"agency": "NASA", "count": 999}


# Test for all retries fail. Should throw exception
def test_get_contract_counts_by_year_all_fail():
    mock_fail = MagicMock()
    mock_fail.raise_for_status.side_effect = requests.exceptions.RequestException("API down")

    # Every attempt fails
    with patch("requests.get", return_value=mock_fail):
        with patch("time.sleep"):
            with pytest.raises(Exception):
                get_contract_counts_by_year_from_api(2024, max_retries=3)


# Test for empty response
def test_get_contract_counts_by_year_empty_results():
    mock_empty = MagicMock()
    mock_empty.raise_for_status.return_value = None
    mock_empty.json.return_value = {"results": []}

    with patch("requests.get", return_value=mock_empty):
        with patch("time.sleep"):
            with pytest.raises(IndexError):  # data["results"][0] will fail
                get_contract_counts_by_year_from_api(2024)
