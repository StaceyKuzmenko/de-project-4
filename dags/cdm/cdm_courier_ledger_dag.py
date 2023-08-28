import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from datetime import datetime, date, time

from examples.cdm.courier_ledger_loader import CourierLedgerLoader


@dag(
    schedule_interval='0/30 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'ledger'],
    is_paused_upon_creation=False
)
def sprint5_case_cdm_courier_ledger():
    @task
    def courier_ledger_load():
        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        rest_loader = CourierLedgerLoader(dwh_pg_connect)
        rest_loader.load_courier_ledger()

    courier_ledger_load()  # type: ignore


my_dag = sprint5_case_cdm_courier_ledger()  # noqa
