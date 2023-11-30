
import sqlalchemy as sa
from urllib.parse import quote
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.operators.python import BranchPythonOperator

from goods_remnants.scripts.collable import etl, date_check


source_con = BaseHook.get_connection('isc')
source_username = source_con.login
source_password = quote(source_con.password)
source_host = source_con.host
source_db = source_con.schema
eng_str = fr'mssql://{source_username}:{source_password}@{source_host}/{source_db}?driver=ODBC Driver 18 for SQL Server&TrustServerCertificate=yes'
source_engine = sa.create_engine(eng_str)

dwh_con = BaseHook.get_connection('vertica')
ps = quote(dwh_con.password)
dwh_engine = sa.create_engine(
    f'vertica+vertica_python://{dwh_con.login}:{ps}@{dwh_con.host}:{dwh_con.port}/sttgaz'
)


default_args = {
    'owner': 'Швейников Андрей',
    'email': ['xxxRichiexxx@yandex.ru'],
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
        'isc_goods_remnants',
        default_args=default_args,
        description='Получение данных из ИСК. Продажи дилеров.',
        start_date=dt.datetime(2023, 10, 31),
        schedule_interval='@daily',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        get_data = PythonOperator(
            task_id=f'get_data',
            python_callable=etl,
                op_kwargs={
                    'source_engine': source_engine,
                    'dwh_engine': dwh_engine,
                },
            )

    with TaskGroup('Загрузка_данных_в_dm_слой') as data_to_dm:

        date_check = BranchPythonOperator(
            task_id='date_check',
            python_callable=date_check,
            op_kwargs={
                'taskgroup': 'Загрузка_данных_в_dm_слой',
                },
        )

        do_nothing = DummyOperator(task_id='do_nothing')
        daily_task = VerticaOperator(
            task_id='daily_task',
            vertica_conn_id='vertica',
            sql='scripts/daily_task.sql',
        )
        collapse = DummyOperator(
            task_id='collapse',
            trigger_rule='none_failed',
        )

        date_check >> [do_nothing, daily_task] >> collapse


    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dm >> end
