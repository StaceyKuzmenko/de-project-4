from typing import Any, Dict, List
from logging import Logger
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from datetime import datetime
import requests
import json

class CourierSaver:
    def save_courier(self, conn: Connection, id: str, update_ts: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.couriers(id, object_value, update_ts)
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

class CourierReader:
    def __init__(self, delivery_api_endpoint: str, headers: Dict) -> None:
        self.delivery_api_endpoint = delivery_api_endpoint
        self.headers = headers
        self.method_url = '/couriers'

    def get_couriers(self) -> List[Dict]:

        params = {
            'sort_field': 'id', 
            'sort_direction': 'asc',
            'offset': 0
            }

        result_list = []

        r = requests.get(self.delivery_api_endpoint + self.method_url, headers=self.headers, params=params)
        while r.text != '[]':
            response_list = json.loads(r.content)

            for courier_dict in response_list:
                result_list.append({'object_id': courier_dict['_id'], 
                                    'object_value': courier_dict,
                                    'update_ts': datetime.now()})
                
                params['offset'] += 1

            r = requests.get(self.delivery_api_endpoint + self.method_url, headers=self.headers, params=params)

        return result_list

class CourierLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    def __init__(self, collection_loader: CourierReader, pg_dest: PgConnect, pg_saver: CourierSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.log = logger

    def run_copy(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            self.log.info(f"starting to load")

            load_queue = self.collection_loader.get_couriers()
            self.log.info(f"Found {len(load_queue)} documents to sync from couriers collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_courier(conn, str(d["object_id"]), d["update_ts"], d['object_value'])

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing couriers.")

            self.log.info(f"Finishing work.")

            return len(load_queue)
