from logging import Logger
from typing import List, Optional
import json
import logging
from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, date, time

log = logging.getLogger(__name__)

class CourierJsonObj(BaseModel):
    id: int
    object_value: str
    update_ts: datetime


class CourierDdsObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str

class CourierOriginRepository:
    def load_raw_courier(self, conn: Connection, last_loaded_record_id: int) -> List[CourierJsonObj]:
        with conn.cursor(row_factory=class_row(CourierJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        replace(object_value, '''', '"') as object_value,
                        update_ts
                    FROM stg.couriers
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs

class CourierDestRepository:

    def insert_courier(self, conn: Connection, courier: CourierDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                    
                """,
                {
                    "courier_id": courier.courier_id,
                    "courier_name": courier.courier_name
                },
            )
    
    def get_courier(self, conn: Connection, courier_id: str) -> Optional[CourierDdsObj]:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id, courier_id, courier_name
                    FROM dds.dm_couriers
                    WHERE courier_id = (%(courier_id)s)
                """,
                {
                    "courier_id": courier_id
                }
            )
            obj = cur.fetchone()
            
        return obj 

class CourierLoader:
    WF_KEY = "courier_from_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    
    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.conn = pg_conn
        self.dds = CourierDestRepository()
        self.raw = CourierOriginRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log
        

    def parser_js(self, raws: List[CourierJsonObj]) -> List[CourierDdsObj]:
        res = []
        for r in raws:
            # log.info('!!LOG2:' + str(r))
            object_value = str(r.object_value).replace("'", '"')
            courier_json = json.loads(object_value)
            t = CourierDdsObj(id=r.id,
                           courier_id=courier_json['_id'],
                           courier_name=courier_json['name']
                           )

            res.append(t)
        return res
    
    def load_couriers(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.conn.connection() as conn:
        
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            loaded_queue = self.raw.load_raw_courier(conn, last_loaded_id)
            loaded_queue.sort(key=lambda x: x.id)
            courier_load = self.parser_js(loaded_queue)

            for courier in courier_load:
                log.info('!!LOG3:' + str(type(courier)))
                log.info('!!LOG4:' + str(courier.courier_id))
                log.info('!!LOG5:' + str(courier.id))
                check_courier = self.dds.get_courier(conn, courier.courier_id)
                if not check_courier:
                    self.dds.insert_courier(conn,courier)
                
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = courier.id
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

                self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")