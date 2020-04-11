import logging
import time
from pykafka.simpleconsumer import OffsetType
from pykafka import KafkaClient

#Creating the kafka client withe bootstrap server config
consumer_client = KafkaClient(hosts="localhost:9092")

#Create the list of topics the consumer will subscript to
topic = consumer_client.topics['sf.crime.stats']

#Creating the consumer 
consumer = topic.get_balanced_consumer(
    consumer_group = 'sf.crime.group',
    auto_commit_enable = True,
    auto_offset_reset = OffsetType.EARLIEST,
    zookeeper_connect = 'localhost:2181'
)

for message in consumer:
    if message is not None:
        print("Kafka Message: ",  message.value, "  ||  Message Offset: " , message.offset)
        time.sleep(2)