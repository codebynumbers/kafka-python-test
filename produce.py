import random
import time
from confluent_kafka import Producer

options = {
    'bootstrap.servers': "localhost:29092",
    'retries': 10,
    'delivery.report.only.error': False,
    'message.max.bytes': 2097152
}
producer = Producer(**options)

with open('/usr/share/dict/words') as fh:
    for line in fh:
        word = line.strip()
        producer.produce('words', word)
        print word
        #time.sleep(random.random())

    # need to force a flush so all data is sent
    producer.flush()