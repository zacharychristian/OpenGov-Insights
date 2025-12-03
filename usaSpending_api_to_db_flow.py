from prefect import flow
from flows.flow_award_by_state import flow_award_by_state
from flows.flow_get_contract_counts_by_agency import flow_contract_counts_by_agency
from flows.flow_get_financial_data import flow_financial_data
from flows.flow_get_top_agencies import flow_top_agencies
from flows.flow_get_top_contracts import flow_top_contracts
from flows.flow_get_ml_data import flow_ml_data

@flow(name="usaSpending API to Postgres DB Orchestrator")
def master_etl_flow():
    flow_award_by_state()
    flow_contract_counts_by_agency()
    flow_financial_data()
    flow_top_agencies()
    flow_top_contracts()
    flow_ml_data()

if __name__ == "__main__":
    master_etl_flow()
