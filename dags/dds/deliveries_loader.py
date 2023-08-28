from logging import Logger
from typing import List, Optional
import json
import logging
from examples.dds.couriers_loader import CourierDdsObj, CourierDestRepository
from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, date, time

log = logging.getLogger(__name__)

class DeliveryJsonObj(BaseModel):
    id: int
    object_value: str
    update_ts: datetime


class DeliveryDdsObj(BaseModel):
    id: int
    order_id: str
    order_ts: datetime
    delivery_id: str
    courier_id: str
    address: str
    delivery_ts: datetime
    rate: float
    sum: float
    tip_sum: float


class DeliveryOriginRepository:
    def load_deliveries(self, conn: Connection, last_loaded_record_id: int) -> List[DeliveryJsonObj]:
        with conn.cursor(row_factory=class_row(DeliveryJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        replace(object_value, '''', '"') as object_value,
                        update_ts
                    FROM stg.deliveries
                    WHERE id > %(last_loaded_record_id)s
                    ORDER BY id ASC;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs

class DeliveryDestRepository:

    def insert_delivery(self, conn: Connection, delivery: DeliveryDdsObj) -> None:
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
                        tip_sum = EXCLUDED.tip_sum
                        ;
                """,
                {
                    "order_id": delivery.order_id,
                    "order_ts": delivery.order_ts,
                    "delivery_id": delivery.delivery_id,
                    "courier_id": delivery.courier_id,
                    "address": delivery.address,
                    "delivery_ts": delivery.delivery_ts,
                    "rate": delivery.rate,
                    "sum": delivery.sum,
                    "tip_sum": delivery.tip_sum
                },
            )

    def get_delivery(self, conn: Connection, delivery_id: str) -> Optional[DeliveryDdsObj]:
        with conn.cursor(row_factory=class_row(DeliveryDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        delivery_id,
                        order_id,
                        order_ts,
                        courier_id,
                        address,
                        delivery_ts,
                        rate,
                        sum,
                        tip_sum
                    FROM dds.dm_deliveries
                    WHERE delivery_id = %(delivery_id)s;
                """,
                {"delivery_id": delivery_id},
            )
            obj = cur.fetchone()
        return obj

class DeliveryLoader:
    WF_KEY = "deliveries_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.dwh = pg
        self.origin = DeliveryOriginRepository()
        self.dds_couriers = CourierDestRepository()
        self.dds_deliveries = DeliveryDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def parse_delivery(self, delivery: DeliveryJsonObj, courier_id: int) -> DeliveryDdsObj:
        delivery_json = json.loads(delivery.object_value)

        t = DeliveryDdsObj(id=0,
                        delivery_id=delivery_json['delivery_id'],
                        order_id=delivery_json['order_id'],
                        order_ts=delivery_json['order_ts'],
                        courier_id=delivery_json['courier_id'],
                        address=delivery_json['address'],
                        delivery_ts=delivery_json['delivery_ts'],
                        rate=delivery_json['rate'],
                        sum=delivery_json['sum'],
                        tip_sum=delivery_json['tip_sum']
                        )

        return t
    
    def load_deliveries(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.origin.load_deliveries(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
            self.log.info('LOG 1:  ' + str(load_queue))
            for delivery in load_queue:

                delivery_json = json.loads(delivery.object_value)
            
                courier = self.dds_couriers.get_courier(conn, delivery_json['courier_id'])
                if not courier:
                    break
              
                delivery_to_load = self.parse_delivery(delivery, courier[0])
                self.dds_deliveries.insert_delivery(conn, delivery_to_load)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = delivery.id
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

                self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")