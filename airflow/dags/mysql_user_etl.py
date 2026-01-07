"""
DAG to demonstrate MySQL operations:
1. Create tbl_user table
2. Insert 10 users
3. Export users to CSV file
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.compat.sdk import Connection
from airflow.providers.mysql.hooks.mysql import MySqlHook
import csv
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def insert_users():
    """Insert 10 users into tbl_user table"""
    mysql_hook = MySqlHook('mysql-test')
    
    users = [
        ('Alice Johnson', 'alice.johnson@example.com', 28),
        ('Bob Smith', 'bob.smith@example.com', 35),
        ('Charlie Brown', 'charlie.brown@example.com', 42),
        ('Diana Prince', 'diana.prince@example.com', 31),
        ('Edward Norton', 'edward.norton@example.com', 45),
        ('Fiona Apple', 'fiona.apple@example.com', 38),
        ('George Wilson', 'george.wilson@example.com', 29),
        ('Hannah Montana', 'hannah.montana@example.com', 25),
        ('Ian McKellen', 'ian.mckellen@example.com', 52),
        ('Jessica Jones', 'jessica.jones@example.com', 33),
    ]

    truncate_query = "TRUNCATE TABLE tbl_user"
    mysql_hook.run(truncate_query)
    
    insert_query = """
        INSERT INTO tbl_user (name, email, age) 
        VALUES (%s, %s, %s)
    """
    
    for user in users:
        mysql_hook.run(insert_query, parameters=user)
    
    print(f"Successfully inserted {len(users)} users into tbl_user")


def export_to_csv():
    """Export users from tbl_user to CSV file"""
    mysql_hook = MySqlHook('mysql-test')
    
    # Create export directory in AIRFLOW_HOME/data
    airflow_home = os.environ.get('AIRFLOW_HOME', os.getcwd())
    export_dir = os.path.join(airflow_home, 'data')
    os.makedirs(export_dir, exist_ok=True)
    
    # Define CSV file path
    csv_file_path = os.path.join(export_dir, f'users_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    
    # Fetch all users
    query = "SELECT id, name, email, age, created_at FROM tbl_user ORDER BY id"
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    
    # Get column names
    column_names = [desc[0] for desc in cursor.description]
    
    # Write to CSV
    with open(csv_file_path, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        # Write header
        csv_writer.writerow(column_names)
        # Write data
        csv_writer.writerows(cursor.fetchall())
    
    cursor.close()
    connection.close()
    
    print(f"Successfully exported users to {csv_file_path}")
    return csv_file_path


with DAG(
    'mysql_user_etl',
    default_args=default_args,
    description='Create table, insert users, and export to CSV using MySQL',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['mysql', 'etl', 'csv'],
) as dag:

    # Task 1: Create tbl_user table
    create_table = SQLExecuteQueryOperator(
        task_id='create_user_table',
        conn_id='mysql-test',
        database='appdb',
        sql="""
            CREATE TABLE IF NOT EXISTS tbl_user (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE,
                age INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    # Task 2: Insert 10 users
    insert_users_task = PythonOperator(
        task_id='insert_users',
        python_callable=insert_users,
    )

    # Task 3: Export users to CSV
    export_users_task = PythonOperator(
        task_id='export_users_to_csv',
        python_callable=export_to_csv,
    )

    # Define task dependencies
    create_table >> insert_users_task >> export_users_task
