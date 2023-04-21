from json import dumps
from kafka import KafkaProducer
from time import sleep

from data import CARS

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda data: dumps(data).encode('utf-8')
)

print('\n\nProducer Started...')
for car in CARS:
    print('\n\nSending car ', car['Name'])
    producer.send('cars_topic', car)
    sleep(2)
