import json
import os
from dotenv import load_dotenv
from faker.proxy import Faker
from flask import Flask
from kafka import KafkaProducer

load_dotenv(verbose=True)

fake = Faker()
fake_user = {
    'first_name': fake.first_name(),
    'last_name': fake.last_name(),
    'birth_date': fake.date_of_birth().isoformat(),
    'email': fake.email()
}

def produce_fake_person():
    producer = KafkaProducer(
        bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send(
        'person',
        value=fake_user,
        key=fake_user['email'].encode('utf-8')
    )

app = Flask(__name__)

if __name__ == '__main__':
    produce_fake_person()
    app.run()