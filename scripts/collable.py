import pandas as pd
import datetime as dt

from sqlalchemy import text


def extract(source_engine, month):
    """Извлечение данных из источника."""

    print('ИЗВЛЕЧЕНИЕ ДАННЫХ')

    with open(
        fr'/home/da/airflow/dags/goods_remnants/scripts/get_data.sql', 'r'
    ) as f:
        command = f.read().format(month=month)

    print(command)

    return pd.read_sql_query(
        command,
        source_engine,
        dtype_backend='pyarrow',
    )


def transform(data, next_execution_date):
    """Преобразование/трансформация данных."""

    print('ТРАНСФОРМАЦИЯ ДАННЫХ')
    print(data)
    
    data.columns = [
        "status",
        "VidTovaraPoDivisionu",
        "Division",
        "ZaiavkaVerhnegoUrovnia_NapravlenieRealizacii",
        "PrognozRealizacii",
        "Zaiavka_Pokupatel",
        "Rezerv",
        "TovarCod65",
        "GruppaCveta",
        "VIN",
        "NomernoiTovar",
        "Zaiavka_MesiacOtgruzki",
        "VariantSborkiProdazi_Facticheskii",
        "PriznakRezervirovaniya",
        "ModelniyGod_Periodicheskiy",
        "GosudarstvenniyContract_IGK",
        "Defect",
        "Kolichestvo",
        "StoimostHraneniyaSNDS",
        "StoimostHraneniyaBezNDS",
        "NDSOtStoimostiHraneniya",
        "PokupatelIzZaiavkiVerhnegoUrovnia",
        "Sklad",
        "VidSklada",
        "DataPrihodaNaSkladGotovogoAM",
        "tovar",
        "comment_HT",     
    ]

    data['load_date'] = next_execution_date
    return data


def load(dwh_engine, data, next_execution_date):
    """Загрузка данных в хранилище."""

    print('ЗАГРУЗКА ДАННЫХ')
    if not data.empty:

        print(data)

        command = f"""
            DELETE
            FROM sttgaz.stage_isc_goods_remnants
            WHERE load_date = '{next_execution_date}';
        """
        print(command)

        dwh_engine.execute(command)

        data.to_sql(
            f'stage_isc_goods_remnants',
            dwh_engine,
            schema='sttgaz',
            if_exists='append',
            index=False,
        )
    else:
        print('Нет новых данных для загрузки.')


def etl(source_engine, dwh_engine, **context):
    """Запускаем ETL-процесс для заданного типа данных."""

    next_execution_date = context['next_execution_date'].date()
    month = context['next_execution_date'].date().replace(day=1)

    data = extract(source_engine, month)
    data = transform(data, next_execution_date)
    load(dwh_engine, data, next_execution_date)


def date_check(taskgroup, **context):
    next_execution_date = context['next_execution_date'].date().replace(day=1)
    if next_execution_date.day >= 1 and next_execution_date.day <= 15:
        return taskgroup + '.' + 'daily_task'
    return taskgroup + '.' + 'do_nothing'

