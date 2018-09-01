import random
import time
from confluent_kafka import Producer

options = {
    'bootstrap.servers': "localhost:29092",
    'retries': 10,
    'delivery.report.only.error': False,  # if set to True, will not be able be able to count successful delivery
    'message.max.bytes': 2097152
}
producer = Producer(**options)

count = 0

def on_delivered(err, msg):
    global count
    if err:
        print err, msg.value()
    else:
        print msg.offset()
        count += 1

with open('/usr/share/dict/words') as fh:
    for line in fh:
        word = line.strip()

        # produce is asynchronous
        producer.produce('words', word, on_delivery=on_delivered)
        #time.sleep(random.random())

    # need to force a flush so all data is sent
    producer.flush()

print "{} messages delivered".format(count)