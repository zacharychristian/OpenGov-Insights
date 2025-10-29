def get_top_agencies_from_api():
    import requests

    """
    Retrieves aggregated contract data for top tier agencies from the USAspending API for the current fiscal year. Outlay amount - final cash disbursement from government. Obligation - legal commitment for contract awards
    """
    url = "https://api.usaspending.gov/api/v2/references/toptier_agencies/"
    params = {
        "order": "desc",
        "sort": "obligated_amount"
    }
    
    # This code attempts to make an API get request with retry logic.  
    # If the request fails, it waits an increasing amount of time (exponential backoff) before retrying,  
    # and raises an exception if all retries are exhausted.
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        return data.get("results", [])
    except requests.exceptions.RequestException as e:
        wait = backoff_factor * (2 ** attempt)
        print(f"API call failed (attempt {attempt+1}/{max_retries}): {e}")
        if attempt < max_retries - 1:
            print(f"Retrying in {wait} seconds...")
            time.sleep(wait)
        else:
            print("All retries failed.")
            raise


def transform_top_agencies():
    """
    Returns a pandas dataframe with aggregated contract data collected for all of the top tier agencies in the current fiscal year.
    """
    import json
    import pandas as pd
    agency_id = []
    toptier_code = []
    agency_name = []
    abbreviation = []
    obligated_amount = []
    active_fy = []
    active_fq = []
    outlay_amount = []
    top_contractors = get_top_agencies_from_api()
    if top_contractors:
        for i, contractor in enumerate(top_contractors): 
            agency_id.append(contractor.get("agency_id", "N/A"))
            toptier_code.append(contractor.get("toptier_code", "N/A"))
            agency_name.append(contractor.get("agency_name", "N/A"))
            abbreviation.append(contractor.get("abbreviation", "N/A"))
            obligated_amount.append(contractor.get("obligated_amount", 0))
            active_fy.append(contractor.get("active_fy", 0))
            active_fq.append(contractor.get("active_fq", 0))
            outlay_amount.append(contractor.get("outlay_amount", 0))
        df = pd.DataFrame(list(zip(agency_id, toptier_code, agency_name, abbreviation, active_fy, active_fq, obligated_amount, outlay_amount)), 
                          columns = ['agency_id', 'toptier_code', 'agency_name', 'abbreviation', 'active_fy', 'active_fq', 'obligated_amount', 'outlay_amount'])
        return df
    else:
        print("No data found or an error occurred.")
        return []
