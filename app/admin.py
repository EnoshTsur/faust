import os

from dotenv import load_dotenv
from flask import Flask
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

load_dotenv(verbose=True)

app = Flask(__name__)


def init_topics():
    client = KafkaAdminClient(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'])

    person_topic = NewTopic(
        name='person',
        num_partitions=int(os.environ['NUM_PARTITION']),
        replication_factor=int(os.environ['NUM_REPLICAS'])
    )

    try:
        pass
        client.create_topics([person_topic])
    except TopicAlreadyExistsError as e:
        print(str(e))
    finally:
        client.close()


if __name__ == '__main__':
    init_topics()
    app.run()
