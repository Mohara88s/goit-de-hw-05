from kafka import KafkaConsumer
from configs import kafka_config
import json

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
)

# Назви топіків
my_name = "vitalii_vasylets"
topic_temperature_alerts_name = f'{my_name}_temperature_alerts'
topic_humidity_alerts_name = f'{my_name}_humidity_alerts'

# Підписка на топіки
consumer.subscribe([topic_temperature_alerts_name,topic_humidity_alerts_name])

print(f"Subscribed to alerts")

# Обробка повідомлень з топіків
try:
    for message in consumer:
        data = message.value
        message = data.get('message')
        sensor_id = data.get('sensor_id')
        print(f"Message from sensor {sensor_id}: {message}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer

