import logging
import pendulum
from logging import Logger
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from examples.stg.couriers_deliveries_dag.couriers_loader import CourierSaver, CourierReader, CourierLoader
from examples.stg.couriers_deliveries_dag.deliveries_loader import DeliverySaver, DeliveryReader, DeliveryLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'example', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_stg_couriers_and_deliveries():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    delivery_api_endpoint = Variable.get("DELIVERY_API_ENDPOINT")

    headers = {
    'X-Nickname': 'StaceyKuzmenko',
    'X-Cohort': '15',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
}
    
    @task()
    def load_couriers():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = CourierSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = CourierReader(delivery_api_endpoint, headers)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = CourierLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()
    
    @task()
    def load_deliveries():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = DeliverySaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = DeliveryReader(delivery_api_endpoint, headers, log)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = DeliveryLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    couriers_loader = load_couriers()
    deliveries_loader = load_deliveries()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    couriers_loader >> deliveries_loader  # type: ignore


couriers_and_deliveries_stg_dag = sprint5_example_stg_couriers_and_deliveries()  # noqa
