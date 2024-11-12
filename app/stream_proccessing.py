import os
from dotenv import load_dotenv
import faust

# Load environment variables
load_dotenv(verbose=True)

# Faust app for stream processing
app = faust.App(
    'person_stream_app',  # App name
    broker=os.environ['BOOTSTRAP_SERVERS'],  # Kafka broker
    value_serializer='json'  # Message value format
)

# Define a Kafka topic to consume from
person_topic = app.topic('person')

# Define an output Kafka topic to produce to
processed_person_topic = app.topic('processed_person')


# Define a data model for the message
class Person(faust.Record, serializer='json'):
    first_name: str
    last_name: str
    birth_date: str
    email: str


# Define a data model for the processed message
class ProcessedPerson(faust.Record, serializer='json'):
    full_name: str
    age: int
    email: str


# Function to calculate age from birth_date
def calculate_age(birth_date: str) -> int:
    from datetime import datetime
    birth_date_obj = datetime.strptime(birth_date, '%Y-%m-%d')
    today = datetime.today()
    return today.year - birth_date_obj.year - ((today.month, today.day) < (birth_date_obj.month, birth_date_obj.day))


# Stream processing agent
@app.agent(person_topic)
async def process_person(messages):
    async for message in messages:
        # Perform some processing
        full_name = f"{message['first_name']} {message['last_name']}"
        age = calculate_age(message['birth_date'])

        # Create a processed person record
        processed_message = ProcessedPerson(
            full_name=full_name,
            age=age,
            email=message['email']
        )

        # Produce the processed message to the output topic
        await processed_person_topic.send(value=processed_message)
        print(f"Processed and sent: {processed_message}")


if __name__ == '__main__':
    # Run Faust in one thread
    app.main()