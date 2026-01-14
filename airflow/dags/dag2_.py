from airflow.sdk import DAG
from datetime import datetime

dag = DAG(
    dag_id='dag_2',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['task_group_test'],
)

with dag:
    from core.task_group_test import my_task_group, task_group_input

    task_a = task_group_input()
    task_b = my_task_group.expand(input_value=task_a)