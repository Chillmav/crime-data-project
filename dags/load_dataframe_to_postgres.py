from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_dataframe_to_postgres(df, table_name, conn_id="crime_data", if_exists="replace", index=False):
    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()
    with engine.begin() as connection:
        df.to_sql(name=table_name, con=connection, if_exists=if_exists, index=index)

