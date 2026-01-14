from airflow.sdk import DAG
from core.tasks import task1, task2, task3, task4

dag = DAG(
    dag_id="test_dag_1",
    schedule=None,
    start_date=None,
    catchup=False,
    params={"a": 5},
    tags=["test_dag_1"],
)

with dag:
    t1 = task1()
    t2 = task2(task1_data=t1)
    t3 = task3(task2_data=t2)
    t4 = task4(task3_data=t3)
