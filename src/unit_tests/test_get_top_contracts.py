import requests
from unittest.mock import MagicMock, patch
from usaSpending_project.src.pipelines.get_top_contracts import (
    get_top_contracts_from_api,
)
import time
import pytest

def test_get_top_contracts_success():
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"results": [{"Award ID": 1, "Award Amount": 1000}]}

    with patch("requests.post", return_value=mock_response):
        with patch("time.sleep"):  # prevent actual waiting
            result = get_top_contracts_from_api(numContracts=10)

    assert result == [{"Award ID": 1, "Award Amount": 1000}]


def test_get_top_contracts_retry_then_success():
    # First call: fails with RequestException
    mock_fail = MagicMock()
    mock_fail.raise_for_status.side_effect = requests.exceptions.RequestException("Temporary failure")

    # Second call: succeeds
    mock_success = MagicMock()
    mock_success.raise_for_status.return_value = None
    mock_success.json.return_value = {"results": [{"Award ID": 2, "Award Amount": 500}]}

    with patch("requests.post", side_effect=[mock_fail, mock_success]):
        with patch("time.sleep"):  # prevent real waiting
            result = get_top_contracts_from_api(numContracts=10, max_retries=3)

    assert result == [{"Award ID": 2, "Award Amount": 500}]



def test_get_top_contracts_all_fail():
    mock_fail = MagicMock()
    mock_fail.raise_for_status.side_effect = requests.exceptions.RequestException("API down")

    with patch("requests.post", return_value=mock_fail):
        with patch("time.sleep"):
            with pytest.raises(requests.exceptions.RequestException):
                get_top_contracts_from_api(numContracts=10, max_retries=3)



def test_get_top_contracts_empty_results():
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"results": []}

    with patch("requests.post", return_value=mock_response):
        with patch("time.sleep"):
            result = get_top_contracts_from_api(numContracts=10)

    assert result == []
