import logging
logger = logging.getLogger(__name__)

def get_top_contracts_from_api(numContracts = 100, max_retries=5, backoff_factor=2):
    import time
    import requests

    """
    https://api.usaspending.gov/api/v2/search/spending_by_award/
    Retrieves the top contracts by award amount descending from the USAspending API for the current fiscal year

    Args:
        numContracts: (int, optional): The number of contracts returned from API
        max_retries: (int, optional): Max number of retry attempts.
        backoff_factor: (int, optional) Base seconds for exponential backoff.

    returns: Returns the 'results' part of the JSON from API call.
    """

    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
        
    payload = {
            "filters": {
                "award_type_codes": ["A", "B", "C", "D"],  # Contract award types
                "agencies": [
                    {
                        "type": "awarding",
                        "tier": "toptier",
                        "name": "Department of Defense"
                    }
                ]
            },
            "fields": ["Award ID", "Recipient Name", "Awarding Agency", "Funding Agency", "Contract Award Type", "Award Amount", "Award Type", "NAICS Code", "Start Date", "End Date"],
            "limit": numContracts,
            "page": 1,
            "sort": "Award Amount",
            "order": "desc"
        }
    
    # This code attempts to make an API POST request with retry logic.  
    # If the request fails, it waits an increasing amount of time (exponential backoff) before retrying,  
    # and raises an exception if all retries are exhausted.
    for attempt in range(max_retries):
        try:
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data["results"]
        except requests.exceptions.RequestException as e:
            # 400 status code in this context means no data to get. 
            if response.status_code == 400:
                logger.info("400 status code. No data to get.")
                return []   
            wait = backoff_factor * (2 ** attempt)
            logger.info(f"API call failed (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                logger.info("All retries failed.")
                raise

def transform_top_contracts(response, numContracts):
    """
    Returns a pandas dataframe with contract data for individual contracts. Data can be from any fiscal year. Ordered by amount awarded descending. The number of contracts received from API is specified by numContracts (can be specified in flow_get_top_contracts.py, but 100 is the standard.

    If empty or null response from API, an empty dataframe is returned.

    Zero or N/A is used for null values in API response
    """
    import pandas as pd

    
    internal_id = []
    award_id = []
    recipient_name = []
    awarding_agency = []
    funding_agency = []
    contract_award_type = []
    award_amount = []
    award_type = []
    naics_code = []
    start_date = []
    end_date = []
    awarding_agency_id = []
    agency_slug = []
    generated_internal_id = []

    # Data cleaned. Data is N/A if null
    if response and len(response) > 0:
        for i in range(numContracts):
            data = response[i] if i < len(response) else {}
            
            internal_id.append(data.get('internal_id', 'N/A'))
            award_id.append(data.get('Award ID', 'N/A'))
            recipient_name.append(data.get('Recipient Name', 'N/A'))
            awarding_agency.append(data.get('Awarding Agency', 'N/A'))
            funding_agency.append(data.get('Funding Agency', 'N/A'))
            contract_award_type.append(data.get('Contract Award Type', 'N/A'))
        
            # Use 0 if missing or not a valid number
            award_amount.append(data.get('Award Amount') if data.get('Award Amount') not in [None, '', 'N/A'] else 0)
        
            award_type.append(data.get('Award Type', 'N/A'))
        
            # Numeric field — default to 0 if missing or empty
            naics_value = data.get('NAICS Code')
            naics_code.append(naics_value if naics_value not in [None, '', 'N/A'] else 0)
        
            # Text/date fields — default to 'N/A' if missing or empty
            start_date.append(data.get('Start Date', 'N/A'))
            end_date.append(data.get('End Date', 'N/A'))
            awarding_agency_id.append(data.get('awarding_agency_id', 'N/A'))
            agency_slug.append(data.get('agency_slug', 'N/A'))
            generated_internal_id.append(data.get('generated_internal_id', 'N/A'))

        logger.info("API calls for get_top_contracts completed.")
        
        df = pd.DataFrame(list(zip(internal_id, award_id, recipient_name, awarding_agency, funding_agency, contract_award_type, award_amount, naics_code,
                                   start_date, end_date, awarding_agency_id, agency_slug, generated_internal_id)),
                          columns = ['internal_id', 'award_id', 'recipient_name', 'awarding_agency', 'funding_agency', 'contract_award_type', 
                                     'award_amount', 'naics_code', 'start_date', 'end_date', 'awarding_agency_id', 'agency_slug', 'generated_internal_id'])
        logger.info("get_top_contracts completed.")
        return df
    else:
        logger.info("No data found or an error occurred.")
        return []