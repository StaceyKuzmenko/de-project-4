from datetime import datetime, timedelta
from lib.dict_util import json2str
from psycopg import Connection
from lib import PgConnect
from psycopg2.extras import execute_values
from typing import List
from pydantic import BaseModel
import json
import logging
import requests

log = logging.getLogger(__name__)

API_KEY = '25c27781-8fde-4b30-a22e-524044a7580f'
NICKNAME = 'StaceyKuzmenko'
COHORT = '15'

API_URL_3 = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries"

url = API_URL_3
headers = {
    'X-Nickname': NICKNAME,
    'X-Cohort': COHORT,
    'X-API-KEY': API_KEY
}
params = {
    'sort_field': 'id',
    'sort_direction': 'asc',
    'limit': 50,
    'offset': 0,
    'from': (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
}


class DeliveryObj(BaseModel):
    object_value: str


class DeliverySaver:

    def __init__(self, pg: PgConnect):
         self.dwh = pg
        
    def get_data(self):
            
        params['offset'] = 0
        params['limit'] = 50
        while True:
            response = requests.get(url, headers=headers, params=params)
            data = response.json()
            result = str(data).replace("'", '"')
            log.info(result)
            # break
            if len(result) < 3:
                break
            else:
                objects = json.loads(result)
                deliveries = []
                for obj in objects:
                    delivery = DeliveryObj(object_value=str(obj))
                    log.info(str(obj))
                    deliveries.append(delivery)
                    
                #self.insert_delivery(deliveries)
                params['offset'] = params['offset'] + params['limit']    
        

    def upload_deliveries(start_date, end_date):
        conn = dwh_hook.get_conn()
        cursor = conn.cursor()
 
        # параметры фильтрации
        start = f"{start_date} 00:00:00"
        end = f"{end_date} 23:59:59"
 
        # идемпотентность
        dwh_hook.run(sql=f"DELETE FROM stg.deliveries WHERE order_ts::date BETWEEN '{start_date}' AND '{end_date}'")
 
        # получение данных
        offset = 0
        while True:
            deliver_rep = requests.get(
                f'https://{base_url}/deliveries/?sort_field=order_ts&sort_direction=asc&from={start}&to={end}&offset={offset}',
                headers=headers).json()
 
            # останавливаемся, когда данные закончились
            if len(deliver_rep) == 0:
                conn.commit()
                cursor.close()
                conn.close()
                task_logger.info(f'Writting {offset} rows')
                break
 
            # запись в БД
            values = [[value for value in deliver_rep[i].values()] for i in range(len(deliver_rep))]
 
            sql = f"INSERT INTO stg.deliveries (order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, " \
                f"sum, tip_sum) VALUES %s "
            execute_values(cursor, sql, values)
 
            offset += len(deliver_rep)