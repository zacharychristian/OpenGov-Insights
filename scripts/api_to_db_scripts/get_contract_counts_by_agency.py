def get_contract_counts_by_year_from_api(fiscal_year, max_retries=5, backoff_factor=2):
  #https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/agency/awards/count.md
    """
    Returns a json containing aggregated contract data by agency for a provided fiscal year
    """
    import requests
    url = f"https://api.usaspending.gov/api/v2/agency/awards/count?fiscal_year={fiscal_year}"

    # This code attempts to make an API get request with retry logic.  
    # If the request fails, it waits an increasing amount of time (exponential backoff) before retrying,  
    # and raises an exception if all retries are exhausted.
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data["results"][0]
        except requests.exceptions.RequestException as e:
            wait = backoff_factor * (2 ** attempt)
            print(f"Attempt {attempt+1}/{max_retries} failed for FY {fiscal_year}: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                print(f"All retries failed for FY {fiscal_year}")
                raise



def get_contract_counts_by_agency():
    """
    Returns a pandas dataframe with all of the data collected from usaSpending API for several fiscal years.
    """
    import json
    import pandas as pd
    awarding_toptier_agency_name = []
    awarding_toptier_agency_code = []
    fisc_year = []
    num_contracts = []
    direct_payments = []
    grants = []
    idvs = []
    loans = []
    other = []
    fiscal_year = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    
    for year in fiscal_year:
        agency_list = get_contract_counts_by_year_from_api(fiscal_year = year)
        for agency in agency_list:
            awarding_toptier_agency_name.append(agency.get('awarding_toptier_agency_name', "N/A"))
            awarding_toptier_agency_code.append(agency.get('awarding_toptier_agency_code', "N/A"))
            fisc_year.append(year)
            num_contracts.append(agency.get('contracts', 0))
            direct_payments.append(agency.get('direct_payments', 0))
            grants.append(agency.get('grants', 0))
            idvs.append(agency.get('idvs', 0))
            loans.append(agency.get('loans', 0))
            other.append(agency.get('other', 0))

    df = pd.DataFrame(list(zip(awarding_toptier_agency_name, awarding_toptier_agency_code, fisc_year, num_contracts, direct_payments,
                         grants, idvs, loans, other)), columns = ['awarding_toptier_agency_name', 'awarding_toptier_agency_code', 'fiscal_year',
                                                                  'num_contracts', 'direct_payments', 'grants', 'idvs', 'loans', 'other'])
    return df
