from unittest.mock import MagicMock, patch
import pytest
from usaSpending_project.dags.src.pipelines.get_award_by_state import (
    get_award_by_state_from_api,
)
import requests
import time

# Test for success on first try
def test_get_award_by_state_success():
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"results": [{"state": "CA", "amount": 123}]}

    with patch("requests.post", return_value=mock_response) as mock_post:
        with patch("time.sleep"):  # prevents real sleep
            results = get_award_by_state_from_api(2024)

    assert results == [{"state": "CA", "amount": 123}]
    mock_post.assert_called_once()


# Test for retry once then success
def test_get_award_by_state_retry_then_success():

    # First call: fails
    mock_fail = MagicMock()
    mock_fail.raise_for_status.side_effect = requests.exceptions.RequestException("Temporary failure")

    # Second call: succeeds
    mock_success = MagicMock()
    mock_success.raise_for_status.return_value = None
    mock_success.json.return_value = {"results": [{"state": "TX", "amount": 500}]}

    with patch("requests.post", side_effect=[mock_fail, mock_success]):
        with patch("time.sleep"):  # prevents actual waiting
            results = get_award_by_state_from_api(2024, max_retries=3)

    assert results == [{"state": "TX", "amount": 500}]


# Test for all retries failed. Exception should be raised
def test_get_award_by_state_all_fail():
    mock_fail = MagicMock()
    mock_fail.raise_for_status.side_effect = Exception("API is down")

    # Every call returns failing response
    with patch("requests.post", return_value=mock_fail):
        with patch("time.sleep"):  # prevent waiting
            with pytest.raises(Exception):
                get_award_by_state_from_api(2024, max_retries=3)


# Test to make sure exception is raised if json results dont include 'results' key
def test_get_award_by_state_missing_results_key():
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"unexpected": "data"}

    with patch("requests.post", return_value=mock_response) as mock_post:
        with patch("time.sleep"):
            with pytest.raises(KeyError):
                get_award_by_state_from_api(2024)

    mock_post.assert_called_once()


# Testing for when the response is empty. Should return [] and not throw an exception.
def test_get_award_by_state_empty_results():
    # Successful response but results list is empty
    mock_empty = MagicMock()
    mock_empty.raise_for_status.return_value = None
    mock_empty.json.return_value = {"results": []}

    with patch("requests.post", return_value=mock_empty):
        with patch("time.sleep"):  # keeps test consistent with others
            results = get_award_by_state_from_api(2024)

    assert results == []

