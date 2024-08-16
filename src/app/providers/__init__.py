from app.providers.kafka.handlers.topic import *
from app.providers.kafka.handlers.generator import handle_generate


def setup_kafka_providers_routes(app):
    app.router.add_post('/kafka/createTopic', handle_create_topic)
    app.router.add_post('/kafka/generateMessages', handle_generate)
