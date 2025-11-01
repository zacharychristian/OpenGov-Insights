# OpenGov-Insights

View the dashboard here: https://opengov-insights-lq4dquz44s9ruz2szrgcbk.streamlit.app/

A full-stack data pipeline that extracts, transforms, loads, and visualizes federal award data using the USAspending API. The project automates data ingestion, performs scalable transformations, stores results in a structured database, and presents interactive visualizations using Streamlit to analyze agency-specific and contractor-level spending.


## Architecture Overview


## Key Insights:
- Department of Health and Human Services has the most contractor spending overall.
- Contractor spending across all agencies has gone up since 2015.
- Contractor spending mostly occurs in Virginia, California, and Texas.
- Virginia has the highest contractor spending per capita by a wide margin.

## Technology Stack:
- Orchestration: Prefect
- Database: PostgreSQL
- Transformation: Python
- Data Source: usaSpending public API
- Language: Python 3.10+, PostgreSQL

## Project Structure:
OpenGov-Insights/<br>
├── usaSpending_api_to_db_flow.py        # Higher Flow script<br>
├── open_gov_insights_dashboard.py<br>
├── scripts/api_to_db_scripts/           # Extracting, Transforming, and Loading scripts<br>
│   ├── __init__.py<br>
│   ├── get_contract_counts_by_agency.py<br>
│   ├── get_award_by_state.py<br>
│   ├── get_top_agencies.py<br>
│   ├── get_top_contracts.py<br>
│   └── get_financial_data.py<br>
│   └── load_data_into_postgres.py<br>
├── flows/                                # Lower orchestration Scripts<br>
│   ├── flow_award_by_state.py  <br>
│   ├── flow_get_contract_counts_by_agency.py<br>
│   ├── flow_get_financial_data.py<br>
│   ├── flow_get_top_agencies.py<br>
│   └── flow_get_top_contracts.py<br>
├── requirements.txt<br>
├── .env<br>
└── data/<br>

## Known Limitations:
- flow_get_financial_data runs slowly. This is because to get the required data, the script pings the API for every agency for every fiscal year requested.
  - In addition to this, the script can only go as fast as the API can send data.
- Data only goes back to 2015
- Some agencies are missing data for certain fiscal years, sometimes several.
  - For example: Federal Communications Commission has no data before 2020 for the Spending Breakdown by Agency visual.
- The Streamlit dashboard uses files instead of database calls. This is because the PostgreSQL database is hosted locally and the Streamlit dashboard is not.

## Pipeline Workflow: 
- Extract: Fetch data from usaSpending API.
- Transform: Parse the raw JSON and convert it into a pandas DataFrame.
- Load: Load transformed data into PostgreSQL database for storage.

### Key Features:
- Exponential backoff for API requests, failed pulls handled gracefully.
- Awards by State data is merged with external datasets (us package data) for the US map functionality.
- Logged all jobs with success/failure status.
- Streamlit dashboard to visualize data.
  - Parquet files used to streamline dashboard dtaa ingestion.

## Future Enhancements:
- Sending transformed data to S3 along with PostgreSQl. This would allow dynamic changes to the data in the Streamlit dashboard, instead of using static files.
- Unit testing
