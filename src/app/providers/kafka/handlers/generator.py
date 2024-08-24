from json import loads

from app.core.generator import (
    MessageGenerator,
)
from app.providers.kafka.managers.producer_manager import (
    KafkaProducerManager,
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
    schedule = data.get("schedule")
    count = data.get("count", 10)
    unique = data.get("unique", False)
    time_period = data.get("time_period", 0)
    session_window = data.get("session_window", 0)

    msg_generator = MessageGenerator(schema=schema, count=count, unique=unique)
    producer = KafkaProducerManager(bootstrap_servers=bootstrap_servers)

    log_data = {
        "topic_name": topic_name,
        "bootstrap_servers": bootstrap_servers,
        "count": count,
        "unique": unique,
        "schedule": schedule,
        "time_period": time_period,
        "session_window": session_window,
    }
    try:
        for msg in msg_generator.generate(
            schedule=schedule,
            time_period=time_period,
            session_window=session_window,
        ):
            producer.publish_msg(topic=topic_name, value=msg)
    except Exception as exc:
        log_data.update({"msg": str(exc)})
        return failed_response(data=log_data)

    log_data.update({"msg": "Message have been published successfully"})
    return success_response(data=log_data)
