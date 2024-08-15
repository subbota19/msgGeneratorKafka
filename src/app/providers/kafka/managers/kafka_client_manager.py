from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from functools import cached_property


class KafkaClientSingleton:
    _instance_dict = {}

    def __new__(cls, bootstrap_servers):
        if bootstrap_servers not in cls._instance_dict:
            kafka_client = super(KafkaClientSingleton, cls).__new__(cls)
            kafka_client._initialize(bootstrap_servers=bootstrap_servers)
            cls._instance_dict[bootstrap_servers] = kafka_client
        return cls._instance_dict[bootstrap_servers]

    def _initialize(self, bootstrap_servers):
        self.admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    def close(self):
        if self.admin_client:
            self.admin_client.close()


class KafkaClientManager:
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers

    @cached_property
    def admin_client(self):
        return KafkaClientSingleton(bootstrap_servers=self.bootstrap_servers).admin_client

    def create_topic(self, topic_name, num_partitions, replication_factor, timeout_ms):
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )

        try:
            print(self.admin_client)
            return self.admin_client.create_topics([topic], timeout_ms=timeout_ms)
        except TopicAlreadyExistsError:
            raise Exception(f"Topic {topic_name} already exists.")
        except Exception as exc:
            raise Exception(f"Failed to create topic {topic_name}: {exc}")

    def close_client(self):
        if hasattr(self, '_admin_client'):
            self.admin_client.close()
