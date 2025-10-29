def get_top_contracts_from_api(numContracts = 100, max_retries=5, backoff_factor=2):
    
    """
    https://api.usaspending.gov/api/v2/search/spending_by_award/
    Retrieves the top contracts by award amount descending from the USAspending API for the current fiscal year
    """
    import requests

    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    
    headers = {
            "Content-Type": "application/json"
        }
        
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
            wait = backoff_factor * (2 ** attempt)
            print(f"API call failed (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                print("All retries failed.")
                raise

def transform_top_contracts(response, numContracts):
    """
    Returns a pandas dataframe with contract data for individual contracts. Data can be from any fiscal year. Ordered by amount awarded descending. The number of contracts received from API is specified by numContracts (can be specified in flow_get_top_contracts.py, but 100 is the standard.
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
        
    for i in range(numContracts): 
        internal_id.append(response[i]['internal_id'])
        award_id. append(response[i]['Award ID'])
        recipient_name.append(response[i]['Recipient Name'])
        awarding_agency.append(response[i]['Awarding Agency'])
        funding_agency.append(response[i]['Funding Agency'])
        contract_award_type.append(response[i]['Contract Award Type'])
        award_amount.append(response[i]['Award Amount'])
        award_type.append(response[i]['Award Type'])
        naics_code.append(response[i]['NAICS Code'] or 0) #Returns NAICS code or 0 if no NAICS code. None type always returns false.
        start_date.append(response[i]['Start Date'])
        end_date.append(response[i]['End Date'])
        awarding_agency_id.append(response[i]['awarding_agency_id'])
        agency_slug.append(response[i]['agency_slug'])
        generated_internal_id.append(response[i]['generated_internal_id'])
    
    df = pd.DataFrame(list(zip(internal_id, award_id, recipient_name, awarding_agency, funding_agency, contract_award_type, award_amount, naics_code,
                               start_date, end_date, awarding_agency_id, agency_slug, generated_internal_id)),
                      columns = ['internal_id', 'award_id', 'recipient_name', 'awarding_agency', 'funding_agency', 'contract_award_type', 
                                 'award_amount', 'naics_code', 'start_date', 'end_date', 'awarding_agency_id', 'agency_slug', 'generated_internal_id'])
    return df
