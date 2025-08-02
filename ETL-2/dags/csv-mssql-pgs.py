from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import text

def transfer_person_to_postgres():
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_conn')
    df = mssql_hook.get_pandas_df("SELECT * FROM Person.Person")

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS person"))

    df.to_sql(
        name='person',
        con=engine,
        schema='person',
        if_exists='replace',
        index=False
    )

default_args = {'start_date': datetime(2025, 8, 1)}

with DAG(
    dag_id='mssql_to_postgres_person_transfer',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['mssql', 'postgres', 'etl']
) as dag:

    transfer_task = PythonOperator(
        task_id='transfer_person',
        python_callable=transfer_person_to_postgres
    )
