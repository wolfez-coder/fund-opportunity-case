import sqlite3
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go

# Use absolute path to avoid path issues
db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'mutal_funds.sqlite')
conn = sqlite3.connect(db_path)

# Overall summary
summary_query = """
SELECT
    ROUND(SUM(client_amount), 2) AS total_client,
    ROUND(SUM(opportunity_amount), 2) AS total_opportunity,
    ROUND(SUM(opportunity_amount - client_amount), 2) AS total_missed_opportunity,
    ROUND(100.0 * SUM(client_amount) / NULLIF(SUM(opportunity_amount), 0), 2) AS capture_pct
FROM normalized_sales_enriched;
"""
summary_df = pd.read_sql_query(summary_query, conn)

# Top territories
territory_query = """
SELECT
    territory,
    ROUND(SUM(client_amount), 2) AS client,
    ROUND(SUM(opportunity_amount), 2) AS opportunity,
    ROUND(SUM(opportunity_amount - client_amount), 2) AS missed_opportunity,
    ROUND(100.0 * SUM(client_amount) / NULLIF(SUM(opportunity_amount), 0), 2) AS capture_pct
FROM normalized_sales_enriched
GROUP BY territory
ORDER BY missed_opportunity DESC
LIMIT 20;
"""
territory_df = pd.read_sql_query(territory_query, conn)

# Top brokers
broker_query = """
SELECT
    broker_name,
    ROUND(SUM(client_amount), 2) AS client,
    ROUND(SUM(opportunity_amount), 2) AS opportunity,
    ROUND(SUM(opportunity_amount - client_amount), 2) AS missed_opportunity,
    ROUND(100.0 * SUM(client_amount) / NULLIF(SUM(opportunity_amount), 0), 2) AS capture_pct
FROM normalized_sales_enriched
GROUP BY broker_name
ORDER BY missed_opportunity DESC
LIMIT 20;
"""
broker_df = pd.read_sql_query(broker_query, conn)

# Top categories
category_query = """
SELECT
    category_name,
    ROUND(SUM(client_amount), 2) AS client,
    ROUND(SUM(opportunity_amount), 2) AS opportunity,
    ROUND(SUM(opportunity_amount - client_amount), 2) AS missed_opportunity,
    ROUND(100.0 * SUM(client_amount) / NULLIF(SUM(opportunity_amount), 0), 2) AS capture_pct
FROM normalized_sales_enriched
GROUP BY category_name
ORDER BY missed_opportunity DESC
LIMIT 20;
"""
category_df = pd.read_sql_query(category_query, conn)

# Overall summary bar chart
fig1 = px.bar(summary_df.melt(id_vars=[], value_vars=['total_client', 'total_opportunity', 'total_missed_opportunity']),
              x='variable', y='value', title='Overall Summary: Client, Opportunity, and Missed Opportunity')
fig1.update_layout(xaxis_title='Metric', yaxis_title='Amount')

# Top territories bar chart
fig2 = px.bar(territory_df, x='territory', y='missed_opportunity', title='Top 20 Territories by Missed Opportunity',
              labels={'missed_opportunity': 'Missed Opportunity Amount'})

# Top brokers bar chart
fig3 = px.bar(broker_df, x='broker_name', y='missed_opportunity', title='Top 20 Brokers by Missed Opportunity',
              labels={'missed_opportunity': 'Missed Opportunity Amount'})

# Top categories bar chart
fig4 = px.bar(category_df, x='category_name', y='missed_opportunity', title='Top 20 Categories by Missed Opportunity',
              labels={'missed_opportunity': 'Missed Opportunity Amount'})

# Show the charts
fig1.show()
fig2.show()
fig3.show()
fig4.show()

# Close connection
conn.close()