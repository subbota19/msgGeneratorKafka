from app.providers.kafka.builds.topic import (
    TopicBuilder,
)
from app.responses.response import (
    failed_response,
    success_response,
)


async def handle_create_topic(request):
    data = await request.post()

    topic_name = data.get("topic_name")
    bootstrap_servers = data.get("bootstrap_servers")

    num_partitions = data.get("num_partitions", 1)
    replication_factor = data.get("replication_factor", 1)
    timeout_ms = data.get("timeout_ms", 10000)

    log_data = {
        "topic_name": topic_name,
        "bootstrap_servers": bootstrap_servers,
        "num_partitions": num_partitions,
        "replication_factor": replication_factor,
        "timeout_ms": timeout_ms,
    }
    try:
        (
            TopicBuilder()
            .set_topic_name(topic_name)
            .set_bootstrap_servers(bootstrap_servers)
            .set_num_partitions(num_partitions)
            .set_replication_factor(replication_factor)
            .set_timeout_ms(timeout_ms)
            .build()
        )
    except Exception as exc:
        log_data.update({"msg": str(exc)})
        return failed_response(data=log_data)

    log_data.update({"msg": "Topic is created"})
    return success_response(data=log_data)
