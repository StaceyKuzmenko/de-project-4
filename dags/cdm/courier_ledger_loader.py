from logging import Logger
from typing import List

from examples.cdm.cdm_settings_repository import EtlSetting, CdmEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, date, time


class CourierLedgerRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_courier_ledger(self) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    truncate table cdm.dm_courier_ledger RESTART IDENTITY;

                    with c as (select 
                                    b.courier_id,
                                    b.order_total_sum,
                                    case when b.rate_avg < 4 and b.courier_initial_order_sum < 100 then 100
	                                    when b.rate_avg >= 4 and b.rate_avg < 4.5 and b.courier_initial_order_sum < 150 then 150
	                                    when b.rate_avg >= 4.5 and b.rate_avg < 4.9 and b.courier_initial_order_sum < 175 then 175
	                                    when b.rate_avg >= 4.9 and b.courier_initial_order_sum < 200 then 200
	                                else courier_initial_order_sum
	                                end courier_order_sum
	                            from (
                                    select 
                                        distinct dd.courier_id,
                                        sum(dd.sum) over (partition by dd.courier_id) order_total_sum,
                                        avg(dd.rate) over (partition by dd.courier_id) as rate_avg,
                                        case when avg(dd.rate) over (partition by dd.courier_id) < 4 then sum(dd.sum) over (partition by courier_id) * 0.05
	                                        when avg(dd.rate) over (partition by dd.courier_id) >= 4 and avg(dd.rate) over (partition by courier_id) < 4.5 then sum(dd.sum) over (partition by courier_id) * 0.07
	                                        when avg(dd.rate) over (partition by dd.courier_id) >= 4.5 and avg(dd.rate) over (partition by courier_id) < 4.9 then sum(dd.sum) over (partition by courier_id) * 0.08
	                                        when avg(dd.rate) over (partition by dd.courier_id) >= 4.9 then sum(dd.sum) over (partition by courier_id) * 0.1
                                        end courier_initial_order_sum
                                    from dds.dm_deliveries dd) as b)
                    insert into cdm.dm_courier_ledger (courier_id, 
							courier_name, 
							settlement_year, 
							settlement_month, 
							orders_count,
							orders_total_sum,
							rate_avg,
							order_processing_fee,
							courier_order_sum,
							courier_tips_sum,
							courier_reward_sum)    
                    select 
                        distinct dd.courier_id,
                        dc.courier_name,
                        extract (year from dd.order_ts) as settlement_year,
                        extract (month from dd.order_ts) as settlement_month,
                        count(dd.order_id) over (partition by dd.courier_id) as orders_count,
                        sum(dd.sum) over (partition by dd.courier_id) order_total_sum,
                        avg(dd.rate) over (partition by dd.courier_id) as rate_avg,
                        sum(dd.sum) over (partition by dd.courier_id) * 0.25 order_processing_fee,
                        c.courier_order_sum,
                        sum(dd.tip_sum) over (partition by dd.courier_id) as courier_tips_sum,
                        (c.courier_order_sum + sum(dd.tip_sum) over (partition by dd.courier_id)) * 0.95 as courier_reward_sum
                    from dds.dm_deliveries dd 
                    left join dds.dm_couriers dc on dd.courier_id = dc.id
                    left join c on dd.courier_id = c.courier_id
                    group by dd.courier_id, dd.order_ts, dc.courier_name, dd.order_id, dd.sum, dd.rate, dd.tip_sum, c.courier_order_sum;
                    """
                )
                conn.commit()


class CourierLedgerLoader:

    def __init__(self, pg: PgConnect) -> None:
        self.repository = CourierLedgerRepository(pg)

    def load_courier_ledger(self):
        self.repository.load_courier_ledger()
