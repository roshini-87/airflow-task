from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def add_values(a, b):
    result = a + b
    print(f"Addition Result: {result}")
    return result

def subtract_values(c, d):
    result = c - d
    print(f"Subtraction Result: {result}")
    return result

def multiply_values(e, f):
    result = e * f
    print(f"Multiplication Result: {result}")
    return result

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'math_operations_dag',
    default_args=default_args,
    description='math operations',
    schedule_interval=None,  
)

# Values for the operations
a, b = 5, 3
c, d = 10, 2
e, f = 4, 7

task_add = PythonOperator(
    task_id='add_values',
    python_callable=add_values,
    op_args=[a, b],
    dag=dag,
)


task_subtract = PythonOperator(
    task_id='subtract_values',
    python_callable=subtract_values,
    op_args=[c, d],
    dag=dag,
)

task_multiply = PythonOperator(
    task_id='multiply_values',
    python_callable=multiply_values,
    op_args=[e, f],
    dag=dag,
)

# Set task dependencies for sequential execution
# task_add >> task_subtract >> task_multiply

# Set tasks to run in parallel
[task_add,task_subtract, task_multiply]
