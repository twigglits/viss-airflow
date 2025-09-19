from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="example_viss_dag",
    description="Example DAG for viss-airflow repo",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Trigger manually via UI
    catchup=False,
    tags=["example", "viss"],
) as dag:
    hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from viss-airflow!'"
    )

    goodbye = BashOperator(
        task_id="say_goodbye",
        bash_command="echo 'Goodbye from viss-airflow!'"
    )

    hello >> goodbye
