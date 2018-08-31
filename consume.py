from confluent_kafka import Consumer
from confluent_kafka.cimpl import KafkaError

c = Consumer({
    'bootstrap.servers': "localhost:29092",
    'group.id': 'mygroup',
    'default.topic.config': {
        'auto.offset.reset': 'smallest'
    }
})

c.subscribe(['words'])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break
    print(u'Received message: {}'.format(msg.value().decode('utf-8')))

c.close()