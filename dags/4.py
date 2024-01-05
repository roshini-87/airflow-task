from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

# Define DAG default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG with default arguments
dag = DAG(
    'purchase_details',
    default_args=default_args,
    description='every 4 minutes',
    schedule_interval='*/4 * * * *',  # Run every 4 minutes
)

# SQL query to calculate total_orders and total_returns for each customer
calculate_purchase_details_sql = """
INSERT INTO Purchase_details (customer_id, total_orders, total_returns, created_time)
SELECT
    o.customer_id,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT pr.return_id) AS total_returns,
    NOW() AS created_time
FROM
    Orders o
LEFT JOIN
    Product_returns pr ON o.customer_id = pr.customer_id
GROUP BY
    o.customer_id;
"""

# Operator to execute the SQL query and insert rows into the Purchase_details table
calculate_purchase_details_task = PostgresOperator(
    task_id='calculate_purchase_details_task',
    postgres_conn_id='postgres_connection',
    sql=calculate_purchase_details_sql,
    dag=dag,
)

# Set task dependencies
calculate_purchase_details_task
