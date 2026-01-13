import logging
logger = logging.getLogger(__name__)

def get_financial_data_from_api(fiscal_year, agency_id, max_retries=5, backoff_factor=2):
    import time
    import requests

    """
    Returns a json containing aggregated contract amounts for a provided agency and fiscal year

    Args:
        fiscal_year: (int) The fiscal year to get data from 
        max_retries: (int, optional): Max number of retry attempts.
        backoff_factor: (int, optional) Base seconds for exponential backoff.

    returns: Returns the 'results' part of the JSON from API call.
    """
    url = (
        f"https://api.usaspending.gov/api/v2/financial_balances/agencies"
        f"?funding_agency_id={agency_id}&fiscal_year={fiscal_year}"
    )

    # This code attempts to make an API get request with retry logic.  
    # If the request fails, it waits an increasing amount of time (exponential backoff) before retrying,  
    # and raises an exception if all retries are exhausted.
    # If the API fulfills the request but there is no data, an empty list is returned
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get("results", [])
        except requests.exceptions.RequestException as e:
            # If the response exists, check the status code safely
            if e.response is not None and e.response.status_code == 400:
                logger.info("400 status code. No data to get.")
                return []
                
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
                
            wait = backoff_factor * (2 ** attempt)
            logger.info(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                logger.info(f"All retries failed for FY {fiscal_year}, agency {agency_id}")
                return []


def transform_financial_data():
    """
    Returns a pandas dataframe with all of the data collected from usaSpending API for several fiscal years. 
    
    Agency IDs are obtained through the first API call. 
    
    If no data is returned from an API call, there is still a row for that agency and year, but the aggregated contract data will be zeroes. 
    """
    import json
    import pandas as pd
    import requests
    # Get all of the agency IDs and agency names
    url = "https://api.usaspending.gov/api/v2/references/toptier_agencies/"
    agency_id = []
    agency_name = []
    try:
        r = requests.get(url)
        resp = r.json().get("results", [])
    except requests.ConnectionError as ce:
        logging.error(f"There was an error with the request, {ce}")
        sys.exit(1)
    
    for agency in resp:
        agency_id.append(agency['agency_id'])
        agency_name.append(agency['agency_name'])

    # Instantiate lists. Lists are used to make a pandas dataframe after all appends since increasing the size of a pandas dataframe is inefficient and slow due to repeated memory reallocation.
    fiscal_year_list = []
    agency_list = []
    agency_name_list = []
    budget_authority_amount = []
    obligated_amount = []
    outlay_amount = []
    fiscal_year = [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    
    #Get financial data for each agency for each year
    for year in fiscal_year:
        for agent in range(len(agency_id)):
            resp = get_financial_data_from_api(year, agency_id[agent])
            fiscal_year_list.append(year)
            agency_list.append(agency_id[agent])
            agency_name_list.append(agency_name[agent])

            #Check if response has data. Append zeroes if no data
            if resp and len(resp) > 0:
                json = resp[0]
                budget_authority_amount.append(json.get('budget_authority_amount', 0))
                obligated_amount.append(json.get('obligated_amount', 0))
                outlay_amount.append(json.get('outlay_amount', 0))
            else: 
                budget_authority_amount.append(0)
                obligated_amount.append(0)
                outlay_amount.append(0)

    logger.info("API calls for financial_data completed.")
    #Make a dataframe from lists
    df = pd.DataFrame(list(zip(fiscal_year_list, agency_list, agency_name_list, budget_authority_amount, obligated_amount, outlay_amount))
                      , columns = ['fiscal_year', 'agency', 'agency_name', 'budget_authority_amount', 'obligated_amount', 'outlay_amount'])
    logger.info("transform_financial_data completed.")
    return df