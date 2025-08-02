from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime

def test_mssql_connection():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    df = hook.get_pandas_df("SELECT TOP 10 * FROM Person.Person")
    print(df)

default_args = {
    'start_date': datetime(2025, 8, 1),
}

with DAG(
    dag_id='test_mssql_connection',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['test', 'mssql']
) as dag:

    test_task = PythonOperator(
        task_id='read_from_mssql',
        python_callable=test_mssql_connection
    )
