import requests
import json
import pandas as pd
import streamlit as st
import altair as alt
from vega_datasets import data
import us


st.set_page_config(layout="wide")

##############
#Title
st.write("# OpenGov Insights Dashboard")
st.write("This dashboard visualizes U.S. government contracting data — including top contracts, agency spending trends, and state-level distributions — to highlight patterns in federal spending over time. All data was sourced from the usaSpending API and processed through a custom data pipeline. It demonstrates end-to-end data engineering and visualization skills for data-focused roles.")
st.write("Link to GitHub Repo with the code for this dashboard and the data pipeline used to populate it: https://github.com/zacharychristian/OpenGov-Insights")
st.write("### Top Contracts and Top Contracting Agencies")
file = 'top_contracts.parquet'
top_contracts = pd.read_parquet(file)

# Top Contracts Bar chart
top_contracts = top_contracts[['recipient_name', 'award_amount', 'start_date','end_date']].rename(columns={'recipient_name': 'Recipient Name', 'award_amount': 'Award Amount', 'start_date': 'Start Date', 'end_date':'End Date'}, inplace=False).sort_values(by = 'Award Amount', ascending = False)

title = alt.TitleParams('Top 10 Contracts', anchor='middle')
top_contracts_chart = alt.Chart(top_contracts).transform_window(
    rank='rank(Award Amount)',
    sort=[alt.SortField('Award Amount', order='descending')]
).transform_filter(
    alt.datum.rank <= 10  # Display top 10 bars
).mark_bar().encode(
    x=alt.X("Award Amount:Q", title = 'Outlay Amount'),
    y=alt.Y("Recipient Name:N", sort='-x'),
    color=alt.Color('Award Amount:Q', scale=alt.Scale(scheme='greens')),
    tooltip=[alt.Tooltip('Recipient Name', title = 'Recipient Name'),
             alt.Tooltip('Award Amount', title = 'Outlay Amount', format="$,.2f"),
            'Start Date', 'End Date']
).properties(
        title = 'Top 10 Individual Contracts of All Time by Outlay Amount',
        width='container',  # let Streamlit control width
        height=400
    )

# Load and prepare data
top_agencies = pd.read_parquet('top_agencies.parquet')[['agency_name', 'obligated_amount']]
top_agencies = top_agencies.rename(columns={'agency_name': 'Agency Name', 'obligated_amount': 'Obligated Amount'})
top_agencies = top_agencies.sort_values(by='Obligated Amount', ascending=False).head(10)
top_agencies_chart = alt.Chart(top_agencies).mark_bar().encode(
    x=alt.X("Obligated Amount:Q"),
    y=alt.Y("Agency Name:O", sort='-x', stack=True),
    color=alt.Color('Obligated Amount:Q', scale=alt.Scale(scheme='blues')),
    tooltip=['Agency Name', alt.Tooltip('Obligated Amount', title = 'Obligated Amount',format="$,.2f")]
).properties(
        title = 'Top 10 Agencies by Sum of All Contracts Obligated Amount 2025',
        width='container',  # let Streamlit control width
        height=400
    )

# Make charts side by side instead of on top of each other
combined_chart = alt.hconcat(top_contracts_chart, top_agencies_chart)

col1, col2 = st.columns([1, 1])  # even columns, centered layout

with col1:
    st.altair_chart(top_contracts_chart, use_container_width=True)

with col2:
    st.altair_chart(top_agencies_chart, use_container_width=True)



###############

st.write("### Government Contract Spending Trends by Agency")

#Obligated amount definition
st.write('Obligated Amount is the total funds that have been formally committed by the government agency to the contractor for a specific period or transaction.') 
st.write('Outlay amount is the actual amount of money that has been spent or paid out to fulfill the terms of a contract, as opposed to the total value of the contract which may be an obligation (a promise to pay).')

financial_data = pd.read_parquet('financial_data.parquet')[['agency_name', 'obligated_amount', 'fiscal_year', 'outlay_amount']]
financial_data = financial_data.rename(columns={'agency_name': 'Agency Name', 'obligated_amount': 'Obligated Amount', 'fiscal_year':'Fiscal Year', 'outlay_amount': 'Outlay Amount'})

# A changeable filter button on streamlit
selected_category = st.selectbox("Select Agency", options=['All'] + list(financial_data['Agency Name'].unique()))
if selected_category != 'All':
    filtered_df = financial_data[financial_data['Agency Name'] == selected_category]
else:
    filtered_df = financial_data

filtered_df['Obligated Amount'] = pd.to_numeric(filtered_df['Obligated Amount'], errors='coerce')

# Create chart
obligated_chart = alt.Chart(filtered_df).mark_line().encode(
    x=alt.X('Fiscal Year:N', axis=alt.Axis(title='Fiscal Year')), # 'O' for ordinal data
    y=alt.Y('Obligated Amount:Q', axis=alt.Axis(title='Obligated Amount')), # 'Q' for quantitative data
    tooltip = [alt.Tooltip('Obligated Amount', format="$,.2f")]
).properties(
    title='Contract Obligated Spending by Agency By Fiscal Year', 
    width = 800,
    height = 600
)
                


###########
#Line chart spending trends (Outlay amount)

filtered_df['Outlay Amount'] = pd.to_numeric(filtered_df['Outlay Amount'], errors='coerce')
outlay_chart = alt.Chart(filtered_df).mark_line().encode(
    x=alt.X('Fiscal Year:N', axis=alt.Axis(title='Fiscal Year')), # 'O' for ordinal data
    y=alt.Y('Outlay Amount:Q', axis=alt.Axis(title='Outlay Amount')), # 'Q' for quantitative data
    color=alt.value("#5D3FD3"),
    tooltip = [alt.Tooltip('Outlay Amount', format="$,.2f")]
).properties(
    title='Contract Outlay Spending by Agency By Fiscal Year', 
    width = 800,
    height = 600
)

# Format charts to be side by side instead of on top of each other
combined_chart = alt.hconcat(outlay_chart, obligated_chart)

col1, col2 = st.columns([1, 1])  # even columns, centered layout

with col1:
    st.altair_chart(outlay_chart, use_container_width=True)

with col2:
    st.altair_chart(obligated_chart, use_container_width=True)

############## State map with data
st.write("### Government Spending by State and Fiscal Year")
st.write("Aggregated amount represents the total dollar value of government contract obligations over a fiscal year.")

# Load state-level data. id is to map data in dataframe to states in the visual
state_awards = pd.read_parquet('awards_by_state.parquet')[[
    'state_abr', 'state', 'fiscal_year', 'aggregated_amount', 'per_capita', 'id'
]]

# Vega topojson for US states
#states = alt.topo_feature(data.us_10m.url, 'states')

# Streamlit fiscal year selector
fiscal_years = sorted(state_awards['fiscal_year'].unique())
selected_year = st.selectbox("Select Fiscal Year", fiscal_years)

# Filter for fiscal year on streamlit page
filtered_df = state_awards[state_awards['fiscal_year'] == selected_year].copy()

# Clean datatypes
filtered_df['aggregated_amount'] = pd.to_numeric(filtered_df['aggregated_amount'], errors='coerce')
filtered_df['per_capita'] = pd.to_numeric(filtered_df['per_capita'], errors='coerce')
filtered_df['fiscal_year'] = filtered_df['fiscal_year'].astype(str)
filtered_df['state'] = filtered_df['state'].astype(str)

state_fips = filtered_df[['state', 'id']]

# Create US Map
chart = (
    alt.Chart(states)
    .mark_geoshape(stroke='white')
    .transform_lookup(
        lookup='id',
        from_=alt.LookupData(filtered_df, 'id', ['state', 'aggregated_amount', 'per_capita', 'fiscal_year'])
    )
    .encode(
        color=alt.Color('aggregated_amount:Q', title='Aggregated Amount', scale=alt.Scale(scheme='blues')),
        tooltip=[
            alt.Tooltip('state:N', title='State'),
            alt.Tooltip('aggregated_amount:Q', title='Aggregated Amount', format="$,.2f"),
            alt.Tooltip('per_capita:Q', title='Per Capita', format="$,.2f"),
            alt.Tooltip('fiscal_year:N', title='Fiscal Year')
        ]
    )
    .properties(
        width=700,
        height=450,
        title=f'US Contract Data for Fiscal Year {selected_year}'
    )
    .project('albersUsa')
)

# Display in Streamlit
st.altair_chart(chart, use_container_width=True)


#########
st.write("### Spending Breakdown by Agency")

#Interactive table
contract_counts = pd.read_parquet('agency_contract_counts.parquet')

agency_filter = sorted(contract_counts['awarding_toptier_agency_name'].unique())
selected_agency = st.selectbox("Select Awarding Agency", agency_filter)

# Filter by fiscal year
filter_table_df = contract_counts[contract_counts['awarding_toptier_agency_name'] == selected_agency].copy()

# Clean datatypes
filter_table_df['num_contracts'] = pd.to_numeric(filter_table_df['num_contracts'], errors='coerce').apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "")
filter_table_df['direct_payments'] = pd.to_numeric(filter_table_df['direct_payments'], errors='coerce').apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "")
filter_table_df['grants'] = pd.to_numeric(filter_table_df['grants'], errors='coerce').apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "")
filter_table_df['idvs'] = pd.to_numeric(filter_table_df['idvs'], errors='coerce').apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "")
filter_table_df['loans'] = pd.to_numeric(filter_table_df['loans'], errors='coerce').apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "")
filter_table_df['other'] = pd.to_numeric(filter_table_df['other'], errors='coerce').apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "")
filter_table_df['awarding_toptier_agency_code'] = pd.to_numeric(filter_table_df['awarding_toptier_agency_code'], errors='coerce')
filter_table_df['fiscal_year'] = filter_table_df['fiscal_year'].astype(str)
filter_table_df['awarding_toptier_agency_name'] = filter_table_df['awarding_toptier_agency_name'].astype(str)

#Rename columns
filter_table_df = filter_table_df.rename(columns={'num_contracts': 'Number of Contracts', 'direct_payments': 'Direct Payments', 'fiscal_year':'Fiscal Year', 'grants': 'Grants', 'idvs': 'IDVS', 'loans': 'Loans', 'other': 'Other', 'awarding_toptier_agency_code': 'Awarding Agency Code', 'awarding_toptier_agency_name': 'Awarding Agency Name'})
st.write('Direct payments are the number of payments made to contractors for a fiscal year.')
st.write('Grants - financial aid awarded to support an organization\'s public-serving mission.')
st.write('IDVs are Indefinite Delivery Vehicles - a flexible, long-term agreement that establishes a framework for future orders of goods or services without specifying the exact quantity, timing, or scope upfront. ')
st.write('Loans - a type of financing that helps businesses bridge the gap between the costs of fulfilling a government contract and being paid by the government.')
st.write(filter_table_df)

