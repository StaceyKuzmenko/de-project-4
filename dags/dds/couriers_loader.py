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

class CourierReader:
    def __init__(self, pg: PgConnect) -> None:
        self.pg_client = pg.client()

    def get_couriers(self, load_threshold: datetime) -> List[Dict]:

        resultlist = []

        with self.pg_client.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_value,
                        update_ts
                    FROM stg.couriers
                    WHERE update_ts > %(load_threshold)s;
                """,
                {"load_threshold": load_threshold},
            )
            rows_list = cur.fetchall()

            for row in rows_list:
                courier_dict = json.loads(row['object_value'])
                resultlist.append({'courier_id': row['id'], 
                                    'name': courier_dict['name'], 
                                    'update_ts': row['update_ts']})

        return resultlist

class CourierSaver:
        def save_courier(self, conn: Connection, courier_id: str, name: str):
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.dm_couriers(courier_id, courier_name)
                        VALUES (%(courier_id)s, %(courier_name)s)
                        ON CONFLICT (courier_id) DO UPDATE
                        SET
                            courier_name = EXCLUDED.courier_name;
                    """,
                    {
                        "courier_id": courier_id,
                        "courier_name": name
                    }
                )

class CourierLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    WF_KEY = "couriers_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: CourierReader, pg_dest: PgConnect, pg_saver: CourierSaver, logger: Logger) -> None:
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

            load_queue = self.collection_loader.get_couriers(last_loaded_ts)
            self.log.info(f"Found {len(load_queue)} documents to sync from couriers collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_courier(conn, str(d["courier_id"]), d["name"])

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing couriers.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
