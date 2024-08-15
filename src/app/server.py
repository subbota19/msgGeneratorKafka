from aiohttp import web
from app.routes import setup_routes
from app.providers import setup_providers_routes
import asyncio
import json


# class MessageHandler:
#     def __init__(self):
#         self.producer = None
#
#     async def start_producer(self, topic):
#         self.producer = KafkaProducer(config.KAFKA_BROKER_URL, topic)
#         await self.producer.start()
#
#     async def stop_producer(self):
#         if self.producer:
#             await self.producer.stop()
#
#     async def handle_generate(self, request):
#         schema = request.query.get('schema')
#         message_size = int(request.query.get('size', 10))
#         threads = int(request.query.get('threads', 1))
#         messages_per_minute = int(request.query.get('messages_per_minute', 60))
#         topic = request.query.get('topic', config.DEFAULT_TOPIC)
#
#         # Convert schema from string to dictionary
#         schema = json.loads(schema)
#
#         await self.start_producer(topic)
#
#         try:
#             tasks = []
#             for _ in range(threads):
#                 tasks.append(self.generate_and_send_messages(schema, message_size, messages_per_minute))
#
#             await asyncio.gather(*tasks)
#
#             return web.json_response({'status': 'success'})
#         finally:
#             await self.stop_producer()
#
#     async def generate_and_send_messages(self, schema, size, rate):
#         generator = MessageGenerator(schema, size)
#         delay = 60.0 / rate
#
#         while True:
#             message = generator.generate_message()
#             await self.producer.send(message)
#             await asyncio.sleep(delay)
#
#     def setup_routes(self, app):
#         app.router.add_get('/generateMessages', self.handle_generate)


def create_app():
    app = web.Application()
    # handler = MessageHandler()
    setup_routes(app)
    setup_providers_routes(app)
    return app
