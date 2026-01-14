from airflow.sdk import task_group, task
from core.tasks import task1, task2, task3, task4

@task
def task_group_input():
    return [1, 2, 3]

@task_group
def my_task_group(input_value):
    # Pass data through the chain explicitly
    t1 = task1(input_value=input_value)
    t2 = task2(task1_data=t1)
    t3 = task3(task2_data=t2)
    t4 = task4(task3_data=t3)