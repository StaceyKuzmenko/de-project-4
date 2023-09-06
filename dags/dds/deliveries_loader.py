from logging import Logger
from typing import List, Optional, Dict
from psycopg.rows import dict_row
import json
import logging
from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from psycopg import Connection
from lib.dict_util import json2str
from datetime import datetime, date, time

log = logging.getLogger(__name__)

class DeliveryReader:
    def __init__(self, pg: PgConnect) -> None:
        self.pg_client = pg.client()

    def get_deliveries(self, load_threshold: datetime, limit, log) -> List[Dict]:

        resultlist = []

        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_value,
                        update_ts
                    FROM stg.deliveries
                    WHERE update_ts > %(load_threshold)s;
                """,
                {"load_threshold": load_threshold},
            )
            rows_list = cur.fetchall()

            for row in rows_list:
                delivery_dict = json.loads(row['object_value'])
                resultlist.append({ 'order_id': self.get_order(delivery_dict['order_id']),
                                    'order_ts': delivery_dict['order_ts'],
                                    'delivery_id': delivery_dict['delivery_id'], 
                                    'address': delivery_dict['address'],
                                    'courier_id': self.get_courier(delivery_dict['courier_id']), 
                                    'delivery_ts': delivery_dict['delivery_ts'],
                                    'rate': delivery_dict['rate'],
                                    'sum': delivery_dict['sum'],
                                    'tip_sum': delivery_dict['tip_sum'],
                                    'update_ts': row['update_ts']})

        return resultlist
    
    def get_courier(self, courier_id):
        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute('select id from dds.dm_couriers where courier_id = %(courier_id)s;',
                {"courier_id": courier_id},
            )
            row = cur.fetchone()
            return row['id']

    def get_order(self, order_key):
        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute('select id from dds.dm_orders where order_key = %(order_key)s;',
                {"order_key": order_key},
            )
            row = cur.fetchone()
            return row['id']

class DeliverySaver:
        def save_delivery(self, conn: Connection, order_id: int, order_ts: datetime, delivery_id: str, courier_id: int, address: str, delivery_ts: datetime, rate: int, sum: float, tip_sum: float):
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.dm_deliveries(order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum)
                    VALUES (%(order_id)s, %(order_ts)s, %(delivery_id)s, %(courier_id)s, %(address)s, %(delivery_ts)s, %(rate)s, %(sum)s, %(tip_sum)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        order_ts = EXCLUDED.order_ts,
                        courier_id = EXCLUDED.courier_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum,
                        tip_sum = EXCLUDED.tip_sum;
                    """,
                    {
                        "order_id": order_id,
                        "order_ts": order_ts,
                        "delivery_id": delivery_id,
                        "courier_id": courier_id,
                        "address": address,
                        "delivery_ts": delivery_ts,
                        "rate": rate,
                        "sum": sum,
                        "tip_sum": tip_sum
                    }
                )

class DeliveryLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 100

    WF_KEY = "deliveries_from_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    
    def __init__(self, collection_loader: DeliveryReader, pg_dest: PgConnect, pg_saver: DeliverySaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = logger
        
    
    def run_copy(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        # JSON ничего не знает про даты. Поэтому записываем строку, которую будем кастить при использовании.
                        # А в БД мы сохраним именно JSON.
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.collection_loader.get_deliveries(last_loaded_ts, self._SESSION_LIMIT, self.log)
            self.log.info(f"Found {len(load_queue)} documents to sync from deliveries collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_delivery(conn, d["order_id"], d["order_ts"], d["delivery_id"], d["courier_id"], d["address"], d["delivery_ts"], d["rate"], d["sum"], d["tip_sum"])

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing deliveries.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
