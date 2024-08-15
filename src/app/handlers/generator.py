from aiohttp import web
from asyncio import gather

import json


async def handle_generate(request):
    print(request.query)
    schema = request.query.get('schema')
    threads = request.query.get('threads')
    topic = request.query.get('topic')
    message_size = request.query.get('size')
    messages_per_minute = int(request.query.get('messages_per_minute', 60))


    # Convert schema from string to dictionary
    schema = json.loads(schema)

    print(schema)

    # await start_producer(topic)

    try:
        tasks = []
        for _ in range(threads):
            pass
            # tasks.append(self.generate_and_send_messages(schema, message_size, messages_per_minute))

        await gather(*tasks)

        return web.json_response({'status': 'success'})
    finally:
        pass
        # await self.stop_producer()
