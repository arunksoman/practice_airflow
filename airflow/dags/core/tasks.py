from airflow.sdk import task


@task
def task1(input_value: int | None = None, **context):
    # get initial value from params if exists
    a = context['params'].get('a', input_value)
    print(f"Initial value of a: {a}")
    return {
        "a": a + 1
    }

@task
def task2(task1_data):
    # get data from previous task via parameter
    return {
        **task1_data,
        "b": 20
    }

@task
def task3(task2_data):
    # get data from previous task via parameter
    return {
        **task2_data,
        "c": 30
    }

@task
def task4(task3_data):
    # get data from previous task via parameter
    total = task3_data['a'] + task3_data['b'] + task3_data['c']
    return {
        **task3_data,
        "total": total
    }