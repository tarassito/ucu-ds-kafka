import logging
import os
from multiprocessing.context import Process

import pandas as pd
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
TOPIC = os.getenv("TOPIC")
DATA_RECORDS = int(os.getenv("DATA_RECORDS"))
PRODUCERS_NUMBER = int(os.getenv("PRODUCERS_NUMBER"))


def on_send_success(record_metadata, producer_id):
    logging.info('Producer [%s] sent message to topic:partition:offset - %s:%d:%d', producer_id,
                 record_metadata.topic, record_metadata.partition, record_metadata.offset)


def produce_data(producer_id, start_record, end_record):
    producer = KafkaProducer(bootstrap_servers=[f'{HOST}:{PORT}'])

    data = pd.read_csv('New-years-resolutions.csv', encoding='latin-1',
                       usecols=['text', 'tweet_created', 'tweet_date', 'tweet_id'])
    data['tweet_id'] = data['tweet_id'].astype(float)
    data = data[start_record:end_record]

    for index, row in data.iterrows():
        message = ','.join([str(elem) for elem in list(row)])
        producer.send(TOPIC, str(message).encode('utf-8')).add_callback(on_send_success, producer_id=producer_id)

    producer.flush()


if __name__ == '__main__':
    if PRODUCERS_NUMBER == 1:
        produce_data(1, 0, DATA_RECORDS)
    elif PRODUCERS_NUMBER == 2:
        processes = []
        process_1 = Process(target=produce_data, args=(2, 0, int(DATA_RECORDS/2)))
        process_1.start()
        processes.append(process_1)

        process_2 = Process(target=produce_data, args=(1, int((DATA_RECORDS/2))+1, DATA_RECORDS+1))
        process_2.start()
        processes.append(process_2)

        # Wait for all processes to finish
        for process in processes:
            process.join()
