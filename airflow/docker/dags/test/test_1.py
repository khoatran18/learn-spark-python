from datetime import date, datetime
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator

with DAG(dag_id='test_1', start_date=datetime.now(), schedule="0 0 * * *") as dag:
    hello = BashOperator(task_id='hello', bash_command='echo hello')

    @task()
    def airflow():
        print("airflow")

    hello >> airflow()