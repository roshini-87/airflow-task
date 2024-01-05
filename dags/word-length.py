from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.models import Variable
def set_airflow_variable(input_list):

    for word in input_list:
        Variable.set(word, len(word))

def print_word_lengths(input_list):

    word_lengths = {word: Variable.get(word) for word in input_list}
    print(word_lengths)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'example_variable_dag',
    default_args=default_args,
    description='setting and retrieving variables',
    schedule_interval=None,  # Set to None to manually trigger the DAG
)

input_list = ["DAG", "variable", "preset"]

# Task 1: Set Airflow variable with input list
task_set_variable = PythonOperator(
    task_id='set_airflow_variable',
    python_callable=set_airflow_variable,
    op_args=[input_list],
    dag=dag,
)

# Task 2: Get variable and print word lengths
task_print_word_lengths = PythonOperator(
    task_id='print_word_lengths',
    python_callable=print_word_lengths,
    op_args=[input_list],
    dag=dag,
)

# Set task dependencies
task_set_variable >> task_print_word_lengths
