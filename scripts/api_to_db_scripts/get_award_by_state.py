#Docs: https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/search/spending_over_time.md

import logging
logger = logging.getLogger(__name__)

def get_award_by_state_from_api(fiscal_year, max_retries=5, backoff_factor=2):
    import requests
    import time

    """
    Returns a json containing aggregated award amounts grouped by state - for a provided fiscal year

    Args:
        fiscal_year: (int) The fiscal year to get data from 
        max_retries: (int, optional): Max number of retry attempts.
        backoff_factor: (int, optional) Base seconds for exponential backoff.

    returns: Returns the 'results' part of the JSON from API call.
    """
    url = "https://api.usaspending.gov/api/v2/search/spending_by_geography/"
    
    # Request payload
    payload = {
        "geo_layer": "state",  # Can also be "county", "district"
        "scope": "place_of_performance",  # Or "recipient_location"
        "filters": {
            "time_period": [
                {
                    "start_date": str(fiscal_year) + "-01-01",
                    "end_date": str(fiscal_year) + "-12-31"
                }
            ],
            "award_type_codes": ["A", "B", "C", "D"]  # These are common codes for contracts/grants
        }
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

def get_award_by_state():
    """  
    Returns a pandas dataframe containing population and government spending data from all US states (No territories). Data is matched to FIPS codes from us package and vega_datasets for the state visual functionality in streamlit.
    
    get_award_by_state_from_api is called multiple times from inside a for loop.

    If no response or nulls in response, zero or N/A is used.
    """
    import json
    import pandas as pd
    from vega_datasets import data
    import us
    import altair as alt

    #Instantiate lists. Lists are used to make a pandas dataframe after all appends since increasing the size of a pandas dataframe is inefficient and slow due to repeated memory reallocation.
    shape_code = []
    state_name = []
    aggregated_amount = []
    population = []
    per_capita = []
    fisc_year = []
    
    fiscal_year = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]

    #Go through json from API call and collect wanted data. Stored in list and finally transferred to pandas dataframe. N/A or zero is used if data null
    for year in fiscal_year:
        state_data = get_award_by_state_from_api(year)
        if state_data and len(state_data) > 0: #Check if response is null
            for state in state_data:
                shape_code.append(state.get('shape_code', "N/A"))
                state_name.append(state.get('display_name', "N/A"))
                aggregated_amount.append(state.get('aggregated_amount', 0))
                population.append(state.get('population', 0))
                per_capita.append(state.get('per_capita', 0))
                fisc_year.append(year)

        else: 
            shape_code.append("N/A")
            state_name.append("N/A")
            aggregated_amount.append(0)
            population.append(0)
            per_capita.append(0)
            fisc_year.append(0)

    logger.info("API calls for get_award_by_state completed")
    df = pd.DataFrame(list(zip(shape_code, state_name, fisc_year, aggregated_amount, population, per_capita)), columns = ['state_abr',
                                          'state', 'fiscal_year', 'aggregated_amount', 'population', 'per_capita'])

    
    # Add ID into the dataframe for the map visualization.
    state_info = alt.Data(url=data.population.url)

    state_fips = pd.DataFrame({
        'state': [s.name for s in us.states.STATES],
        'id': [int(s.fips) for s in us.states.STATES]
    })

    # Clean datatypes
    df['aggregated_amount'] = pd.to_numeric(df['aggregated_amount'], errors='coerce')
    df['per_capita'] = pd.to_numeric(df['per_capita'], errors='coerce')
    df['fiscal_year'] = df['fiscal_year'].astype(str)
    df['state'] = df['state'].astype(str)

    
    # Merge FIPS codes into our filtered data
    df = df.merge(state_fips, on='state', how='left')
    df = df.dropna()
    df['id'] = df['id'].astype(int)
    logger.info("get_award_by_state completed")
    return df
