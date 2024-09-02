import os
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

def get_sys_objects_list():
    mssqlh = MsSqlHook(mssql_conn_id = 'mssql_af_net')
    query = \
        r"""
        use BU83_BIVIEW1;
        select
            object_id,
            name,
            type,
            1 ct__load
            from sys.objects
            where type in ('U', 'V')
            order by 3,2
        """
    res = mssqlh.get_records(sql=query)
    return res

with DAG(dag_id='test_mssql_conn', start_date=datetime(2024,8,19), schedule=None,
         catchup=False, max_active_runs=1) as dag:
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    get_obj_list = PythonOperator(
        task_id = 'get_obj_list',
        python_callable = get_sys_objects_list
    )

    start >> get_obj_list >> end



