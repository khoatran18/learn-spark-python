import sys

from confluent_kafka import Consumer, Producer, KafkaError
import socket

### Producer and Consumer Factory
def create_producer():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname(),
    }
    producer = Producer(conf)
    return producer

def default_callback_commit_consumer(err, partition):
    if err:
        print('Commited failed: %s' % err)
    else:
        print("Committed success: " + str(partitions))

def create_consumer(group, callback=default_callback_commit_consumer):
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': group,
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest',
        'on_commit': callback,
    }
    consumer = Consumer(conf)

    return consumer

### Consume loop
def consume_loop(consumer, topics, msg_process, min_commit_count=10, timeout_poll=1):
    try:
        consumer.subscribe(topics)
        msg_count = 0
        while running:
            msg = consumer.poll(timeout=timeout_poll)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n'
                                     % (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
                msg_count += 1
                if msg_count % min_commit_count == 0:
                    consumer.commit(asynchrnous=True)
    finally:
        consumer.close()