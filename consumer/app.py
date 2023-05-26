import csv
import logging
import os
import time
from multiprocessing.context import Process

from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
TOPIC = os.getenv("TOPIC")
CONSUMERS_NUMBER = int(os.getenv("CONSUMERS_NUMBER"))


def run_consumer(consumer_id: int):
    count = 0
    consumer = KafkaConsumer(TOPIC,
                             bootstrap_servers=[f'{HOST}:{PORT}'],
                             consumer_timeout_ms=10000,
                             auto_offset_reset='earliest', #'latest'
                             enable_auto_commit=True,
                             group_id="first")

    with open(f'logs/log_{consumer_id}.csv', 'w') as f:
        writer = csv.writer(f)
        logging.info("Consumer [%s] is reading messages from topic [%s]...", consumer_id, TOPIC)
        for count, message in enumerate(consumer):
            logging.info("%d:%s:%d:%d:%d value=%s", consumer_id, message.topic, message.partition,
                         message.offset, message.timestamp, message.value)

            time.sleep(1)
            writer.writerow([int(message.timestamp/1000), int(time.time()),
                             str(message.value), len(str(message.value))])
        logging.info(f"Consumer [%s] read %d messages", consumer_id, count)


if __name__ == '__main__':
    # Create and start multiple consumer processes
    processes = []
    for consumer_id in range(CONSUMERS_NUMBER):
        process = Process(target=run_consumer, args=(consumer_id,))
        process.start()
        processes.append(process)

    # Wait for all processes to finish
    for process in processes:
        process.join()
