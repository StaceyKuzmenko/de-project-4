from typing import Any, Dict, List
from logging import Logger
from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from datetime import datetime
import requests
import json

class DeliverySaver:
    def save_delivery(self, conn: Connection, id: str, update_ts: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliveries(id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
                {
                    "id": id,
                    "val": str_val,
                    "update_ts": update_ts
                }
            )

class DeliveryReader:
    def __init__(self, delivery_api_endpoint: str, headers: Dict, log) -> None:
        self.delivery_api_endpoint = delivery_api_endpoint
        self.headers = headers
        self.method_url = '/deliveries'
        self.log = log

    def get_deliveries(self, load_threshold: datetime) -> List[Dict]:

        params = {
            'from': load_threshold.strftime("%Y-%m-%d %H:%M:%S"),
            'sort_field': 'id', 
            'sort_direction': 'asc',
            'offset': 0
            }

        result_list = []

        r = requests.get(self.delivery_api_endpoint + self.method_url, headers=self.headers, params=params)
        while r.text != '[]':
            self.log.info(str(r.content))
            self.log.info(str(r.url))
            response_list = json.loads(r.content)

            for delivery_dict in response_list:
                result_list.append({'object_id': delivery_dict['delivery_id'], 
                                    'object_value': delivery_dict,
                                    'update_ts': datetime.fromisoformat(delivery_dict['delivery_ts'])})
                
                params['offset'] += len(response_list)

            r = requests.get(self.delivery_api_endpoint + self.method_url, headers=self.headers, params=params)

        return result_list

class DeliveryLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    WF_KEY = "example_deliveries_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: DeliveryReader, pg_dest: PgConnect, pg_saver: DeliverySaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
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
                        self.LAST_LOADED_TS_KEY: datetime(2023, 8, 2).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            self.log.info(f"starting to load")

            load_queue = self.collection_loader.get_deliveries(last_loaded_ts)
            self.log.info(f"Found {len(load_queue)} documents to sync from deliveries collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_delivery(conn, str(d["object_id"]), d["update_ts"], d['object_value'])

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing deliveries.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
