from aiohttp import web

from app.providers import (
    setup_kafka_providers_routes,
)
from app.routes import (
    setup_routes,
)


def create_app():
    app = web.Application()
    setup_routes(app)
    setup_kafka_providers_routes(app)
    return app
