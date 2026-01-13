import logging
logger = logging.getLogger(__name__)

def get_ml_data_from_api(fiscal_year = 2024, quarter = 1, max_retries=5, backoff_factor=2):
    import time
    import requests

    """
    https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/spending.md
    Retrieves data from agency for amount. 
    The amount in the API response represents aggregated spending (i.e., obligations), not necessarily the full face value of awards.

    Args:
        fiscal_year: (int, optional) The fiscal year to get data from 
        quarter: (int, optional) The quarter 1-4 to get data from
        max_retries: (int, optional): Max number of retry attempts.
        backoff_factor: (int, optional) Base seconds for exponential backoff.
    """
    

    url = "https://api.usaspending.gov/api/v2/spending/"
    
    payload =   {
      "type":"agency",
      "filters": {
          "fy":str(fiscal_year),
          "quarter":str(quarter),
          "budget_function":"050" #050 is for national defense. Other budget functions can be found via this API: "https://api.usaspending.gov/api/v2/budget_functions/list_budget_functions/"
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
            if e.response is not None and e.response.status_code == 400:
                logger.info("400 status code. No data to get.")
                return []        
            
            wait = backoff_factor * (2 ** attempt)
            logger.info(f"API call failed (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                logger.info("All retries failed.")
                return []



def get_ml_data(last_fiscal_year = 2025):
    """
    Returns a pandas dataframe with agency name, agency code, fiscal year, quarter, and amount of government spending over the time period (fiscal year and quarter)

    If no response or nulls in response, zero or N/A is used.
    """
    import json
    import pandas as pd
    import requests
    
    amount = []
    fiscal_year = []
    quarter_list = []
    name = []
    code = []

    for year in range(2015, last_fiscal_year+1): 
        for quarter in range(1,5): #Only 4 quarters in a year. 1,2,3,4
            resp = get_ml_data_from_api(fiscal_year = year, quarter = quarter)         

            #Check if response has data. Append zeroes if no data
            if resp and len(resp) > 0:
                for i in range(0,len(resp)): # Go through list of json
                    amount.append(resp[i].get('amount',0))
                    name.append(resp[i].get('name','N/A'))
                    code.append(str(resp[i].get('code', '')))
                    fiscal_year.append(year)
                    quarter_list.append(quarter)
            else:
                fiscal_year.append(year)
                quarter_list.append(quarter)
                amount.append(0)
                name.append('N/A')
                code.append('')

    logger.info("API calls for get_ml_data completed.")
    #Make a dataframe from lists
    df = pd.DataFrame(list(zip(amount, fiscal_year, quarter_list, name, code))
                      , columns = ['amount', 'fiscal_year', 'quarter', 'name', 'code'])
    
    logger.info("get_ml_data completed.")
    return df