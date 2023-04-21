from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'cars_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='cars-group-id',
    value_deserializer=lambda data: loads(data.decode('utf-8'))
)

print('\n\nConsumer started...')
for message in consumer:
    print('\n\n------->topic', message.topic)
    print('\n------->partition', message.partition)
    print('\n------->offset', message.offset)
    print('\nReceived car: ', message.value)