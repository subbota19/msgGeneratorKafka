from app.handlers.generator import *


def setup_routes(app):
    app.router.add_get('/generateMessages', handle_generate)
