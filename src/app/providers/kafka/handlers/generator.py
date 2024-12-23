from json import loads

from app.core.asyncio_generator import AsyncioGenerator
from app.core.generator import (
    MessageGenerator,
)
from app.providers.kafka.managers.aio_producer_manager import (
    AIOKafkaProducerManager,
)
from app.responses.response import (
    failed_response,
    success_response,
)


async def handle_generate(request):
    data = await request.post()

    topic_name = data.get("topic_name")
    bootstrap_servers = data.get("bootstrap_servers")
    schema = loads(data.get("schema", {}))
    count = int(data.get("count", 10))
    unique = data.get("unique", False)
    parallelism = int(data.get("parallelism", 1))
    time_period = int(data.get("time_period", 1))
    session_window = int(data.get("session_window", 1))

    msg_generator = MessageGenerator(schema=schema, count=count, unique=unique)
    producer = AIOKafkaProducerManager(bootstrap_servers=bootstrap_servers)

    log_data = {
        "topic_name": topic_name,
        "bootstrap_servers": bootstrap_servers,
        "count": count,
        "unique": unique,
        "time_period": time_period,
        "session_window": session_window,
    }
    try:
        from time import time

        s = time()
        generator = AsyncioGenerator(
            producer=producer,
            message_generator=msg_generator,
            parallelism=parallelism,
            time_period=time_period,
            session_window=session_window,
            topic_name=topic_name,
        )
        await generator.generate()

        print(f"Time: {time() - s}")

    except Exception as exc:
        log_data.update({"msg": str(exc)})
        return failed_response(data=log_data)

    log_data.update({"msg": "Message have been published successfully"})
    return success_response(data=log_data)
