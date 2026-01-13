############Top 10 government agencies by total contract moneys committed to (obligated amount)
#https://api.usaspending.gov/docs/endpoints
#Notes:
#Outlay amount - final cash disbursement from government
#Obligation - legal commitment for contract awards

import logging
logger = logging.getLogger(__name__)

def get_top_agencies_from_api(max_retries=5, backoff_factor=2):
    import requests
    import time


    """
    Retrieves aggregated contract data for top tier agencies from the USAspending API for the current fiscal year

    Args:
        max_retries: (int, optional): Max number of retry attempts.
        backoff_factor: (int, optional) Base seconds for exponential backoff.

    returns: Returns the 'results' part of the JSON from API call.
    """
    url = "https://api.usaspending.gov/api/v2/references/toptier_agencies/"
    params = {
        "order": "desc",
        "sort": "obligated_amount"
    }
    
    # This code attempts to make an API get request with retry logic.  
    # If the request fails, it waits an increasing amount of time (exponential backoff) before retrying,  
    # and raises an exception if all retries are exhausted.
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()  # Raise an exception for HTTP errors
            data = response.json()
            return data.get("results", [])
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


def get_top_agencies():
    """
    Returns a pandas dataframe with aggregated contract data collected for all of the top tier agencies in the current fiscal year.

    If null API call response or nulls in response, zeroes or N/A is used.
    """
    import json
    import pandas as pd


    # Instantiate lists. Lists are used to make a pandas dataframe after all appends since increasing the size of a pandas dataframe is inefficient and slow due to repeated memory reallocation.
    agency_id = []
    toptier_code = []
    agency_name = []
    abbreviation = []
    obligated_amount = []
    active_fy = []
    active_fq = []
    outlay_amount = []
    
    top_contractors = get_top_agencies_from_api()

    #top_contractors is 1 API call. No need for fiscal years. If expanded, might need logic for different pages
    if top_contractors and len(top_contractors) > 0:
        for i, contractor in enumerate(top_contractors): 
            agency_id.append(contractor.get("agency_id", "N/A"))
            toptier_code.append(contractor.get("toptier_code", "N/A"))
            agency_name.append(contractor.get("agency_name", "N/A"))
            abbreviation.append(contractor.get("abbreviation", "N/A"))
            obligated_amount.append(contractor.get("obligated_amount", 0))
            active_fy.append(contractor.get("active_fy", 0))
            active_fq.append(contractor.get("active_fq", 0))
            outlay_amount.append(contractor.get("outlay_amount", 0))

        logger.info("API calls for transform_top_agencies completed.")

        df = pd.DataFrame(list(zip(agency_id, toptier_code, agency_name, abbreviation, active_fy, active_fq, obligated_amount, outlay_amount)), 
                          columns = ['agency_id', 'toptier_code', 'agency_name', 'abbreviation', 'active_fy', 'active_fq', 'obligated_amount', 'outlay_amount'])
        logger.info("transform_top_agencies completed.")
        return df
    else:
        logger.info("No data found or an error occurred.")
        return []