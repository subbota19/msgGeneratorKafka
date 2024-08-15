from app.providers.kafka.handlers.topic import *


def setup_providers_routes(app):
    app.router.add_post('/createTopic', handle_create_topic)
