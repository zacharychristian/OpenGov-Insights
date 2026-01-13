import requests
from unittest.mock import MagicMock, patch
from usaSpending_project.dags.src.pipelines.get_top_agencies import (
    get_top_agencies_from_api,
)
import requests
import time
import pytest

# Test for success on first try
def test_get_top_agencies_success():
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"results": [{"toptier_agency_name": "DOD", "obligated_amount": 1000}]}

    with patch("requests.get", return_value=mock_response):
        result = get_top_agencies_from_api()

    assert result == [{"toptier_agency_name": "DOD", "obligated_amount": 1000}]

# Test for retry once then success
def test_get_top_agencies_retry_then_success():
    # First call: fails with RequestException
    mock_fail = MagicMock()
    mock_fail.raise_for_status.side_effect = requests.exceptions.RequestException("Temporary failure")

    # Second call: succeeds
    mock_success = MagicMock()
    mock_success.raise_for_status.return_value = None
    mock_success.json.return_value = {"results": [{"toptier_agency_name": "NASA", "obligated_amount": 500}]}

    with patch("requests.get", side_effect=[mock_fail, mock_success]):
        with patch("time.sleep"):  # prevent real waiting
            result = get_top_agencies_from_api(max_retries=3)

    assert result == [{"toptier_agency_name": "NASA", "obligated_amount": 500}]


# Testing for when the response is empty. Should return [] and not throw an exception.
def test_get_top_agencies_empty_results():
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"results": []}

    with patch("requests.get", return_value=mock_response):
        result = get_top_agencies_from_api()

    assert result == []


# Test for all retries failed. Exception should be raised
def test_get_top_agencies_all_fail():
    mock_fail = MagicMock()
    mock_fail.raise_for_status.side_effect = requests.exceptions.RequestException("API down")

    with patch("requests.get", return_value=mock_fail):
        with patch("time.sleep"):
            with pytest.raises(requests.exceptions.RequestException):
                get_top_agencies_from_api(max_retries=3)

