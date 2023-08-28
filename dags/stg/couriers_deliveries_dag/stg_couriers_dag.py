import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from examples.stg.couriers_deliveries_dag.couriers_saver import CourierSaver
from lib import ConnectionBuilder


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'example', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_stg_couriers():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_couriers():
        # Инициализируем класс, в котором реализована логика сохранения.
        courier_obj = CourierSaver(dwh_pg_connect)
        courier_obj.get_data()

    couriers_loader = load_couriers()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    couriers_loader  # type: ignore


couriers_stg_dag = sprint5_example_stg_couriers()  # noqa
