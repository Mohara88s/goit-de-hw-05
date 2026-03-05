from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json
import uuid

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

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назви топіків
my_name = "vitalii_vasylets"
topic__building_sensors_name = f'{my_name}_building_sensors'
topic_temperature_alerts_name = f'{my_name}_temperature_alerts'
topic_humidity_alerts_name = f'{my_name}_humidity_alerts'

def process_message(message):
    try:
        data = message.value

        sensor_id = data.get('sensor_id')
        temperature = data.get('temperature')
        humidity = data.get('humidity')

        if temperature > 40:
            alert_data = {
                **data,
                "message": f"Temperature {temperature} exceeds 40 degrees Celsius"
            }
            producer.send(topic_temperature_alerts_name, key=str(uuid.uuid4()), value=alert_data)
            producer.flush()
            print(f"Alert message from sensor {sensor_id} with temperature {temperature} sent successfully.")
    
        if humidity < 20 or humidity > 80:
            alert_data = {
                **data,
                "message": f"Humidity {humidity} outside the range of 20% to 80%"
            }
            producer.send(topic_humidity_alerts_name, key=str(uuid.uuid4()), value=alert_data)
            producer.flush()
            print(f"Alert message from sensor {sensor_id} with humidity {humidity} sent successfully.")

        print(f"Message from sensor {sensor_id} with temperature {temperature} & humidity {humidity} processed successfully" )
    except Exception as e:
        print(f"An error occurred: {e}")

# Підписка на тему
consumer.subscribe([topic__building_sensors_name])

print(f"Subscribed to topic '{topic__building_sensors_name}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        process_message(message)
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer

