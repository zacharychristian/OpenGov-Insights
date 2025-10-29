def get_financial_data_from_api(fiscal_year, agency_id, max_retries=5, backoff_factor=2):
    """
    Returns a json containing aggregated contract amounts for a provided agency and fiscal year
    """
    import requests
    url = (
        f"https://api.usaspending.gov/api/v2/financial_balances/agencies"
        f"?funding_agency_id={agency_id}&fiscal_year={fiscal_year}"
    )

    # This code attempts to make an API get request with retry logic.  
    # If the request fails, it waits an increasing amount of time (exponential backoff) before retrying,  
    # and raises an exception if all retries are exhausted.
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get("results", [])
        except requests.exceptions.RequestException as e:
            wait = backoff_factor * (2 ** attempt)
            print(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                print(f"All retries failed for FY {fiscal_year}, agency {agency_id}")
                return []


def transform_financial_data():
    """
    Returns a pandas dataframe with all of the data collected from usaSpending API for several fiscal years. Agency IDs are obtained through the first API call. If no data is returned from an API call, there is still a row for that agency and year, but the aggregated contract data will be zeroes. 
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

    # Instantiate lists
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
            if resp:
                json = resp[0]
                budget_authority_amount.append(json['budget_authority_amount'])
                obligated_amount.append(json['obligated_amount'])
                outlay_amount.append(json['outlay_amount'])
            else: 
                budget_authority_amount.append(0)
                obligated_amount.append(0)
                outlay_amount.append(0)
    
    #Make a dataframe from lists
    df = pd.DataFrame(list(zip(fiscal_year_list, agency_list, agency_name_list, budget_authority_amount, obligated_amount, outlay_amount))
                      , columns = ['fiscal_year', 'agency', 'agency_name', 'budget_authority_amount', 'obligated_amount', 'outlay_amount'])
    return df
