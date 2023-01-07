import json
from kafka import KafkaConsumer
from datetime import datetime
import pytz

print("Waiting on Messages ...")
if __name__ == '__main__':
    consumer = KafkaConsumer(
        'dbserver1.inventory.customers',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest'
    )

    paris_tz = pytz.timezone('Europe/Paris')
    for message in consumer:

        consumer_data = json.loads(message.value)
        op = consumer_data['payload']['op']
        ts_ms = float(consumer_data['payload']['ts_ms'])/1000
        # convert to datetime
        ts_str=datetime.fromtimestamp(ts_ms).strftime('%Y-%m-%d %H:%M:%S:%f')
        ts_str_utc=datetime.utcfromtimestamp(ts_ms).strftime('%Y-%m-%d %H:%M:%S:%f')
        ts_str_tz=datetime.utcfromtimestamp(ts_ms).astimezone(paris_tz).strftime('%Y-%m-%d %H:%M:%S:%f')
        id = consumer_data['payload']['after']['id']
        first_name = consumer_data['payload']['after']['first_name']
        last_name = consumer_data['payload']['after']['last_name']
        email = consumer_data['payload']['after']['email']

        print(ts_ms,  "TS > ",ts_str, "TS UTC > ", ts_str_utc, "TS Local Time >", ts_str_tz)
        print(op, id, first_name, last_name, email)
        print("====== payload =====")
        print(consumer_data['payload'])
        print("====== schema =====")
        print(consumer_data['schema'])
        print("===================")
        print(consumer_data)
        print(ts_ms,  "TS > ",ts_str, "TS UTC > ", ts_str_utc, "TS Local Time >", ts_str_tz)
        print("===================")
        print(op, id, first_name, last_name, email)
        print("======"*15)
