import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.couriers_loader import CourierSaver, CourierReader, CourierLoader
from examples.dds.deliveries_loader import DeliverySaver, DeliveryReader, DeliveryLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'ddl', 'dds', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dds_couriers_and_deliveries_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task()
    def load_couriers():
        pg_saver = CourierSaver()
        collection_reader = CourierReader(dwh_pg_connect)
        loader = CourierLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    @task
    def load_deliveries():
        pg_saver = DeliverySaver()
        collection_reader = DeliveryReader(dwh_pg_connect)
        loader = DeliveryLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

  # Инициализируем объявленные таски.
    couriers_dict = load_couriers()
    deliveries_dict = load_deliveries()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    couriers_dict >> deliveries_dict  # type: ignore

dds_couriers_and_deliveries_dag = sprint5_dds_couriers_and_deliveries_dag()
