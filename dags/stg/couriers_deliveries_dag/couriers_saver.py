from typing import List

from lib.dict_util import json2str
from psycopg import Connection
from psycopg2.extras import execute_values
from lib import PgConnect
from pydantic import BaseModel
import json
import logging
import requests

API_KEY = '25c27781-8fde-4b30-a22e-524044a7580f'
NICKNAME = 'StaceyKuzmenko'
COHORT = '15'

API_URL_2 = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers"

log = logging.getLogger(__name__)
url = API_URL_2
headers = {
    'X-Nickname': NICKNAME,
    'X-Cohort': COHORT,
    'X-API-KEY': API_KEY
}
params = {
    'sort_field': '_id',
    'sort_direction': 'asc',
    'limit': 50,
    'offset': 0
}

class CourierObj(BaseModel):
    object_value: str


class CourierSaver:

    def __init__(self, pg: PgConnect):
         self.dwh = pg
        

    def get_data(self):
            
        params['offset'] = 0
        params['limit'] = 50

        while True:
            response = requests.get(url, headers=headers, params=params)
            data = response.json()
            result = str(data).replace("'", '"')
            if len(result) < 3:
                break
            else:
                objects = json.loads(result)
                couriers = []
                for obj in objects:
                    courier = CourierObj(object_value=str(obj))
                    log.info(str(obj))
                    couriers.append(courier)
                    
                self.insert_courier(couriers)
                params['offset'] = params['offset'] + params['limit']

    def insert_courier(self, couriers: List[CourierObj]) -> None:
        with self.dwh.connection() as conn:
            with conn.cursor() as cur:
                for courier in couriers:
                        cur.execute(
                        """
                            INSERT INTO stg.couriers(
                                object_value, 
                                update_ts)
                            VALUES (
                                %(object_value)s,
                                NOW())
                        """,
                        {
                            "object_value": courier.object_value
                        },
                    )