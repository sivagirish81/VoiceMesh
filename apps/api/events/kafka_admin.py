import asyncio

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

TOPICS = [
    "call-events",
    "pipeline-events",
    "provider-events",
    "outbox-events",
    "usage-events",
    "billing-events",
    "dead-letter-events",
]


async def create_topics(bootstrap_servers: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        topics = [NewTopic(name=topic, num_partitions=3, replication_factor=1) for topic in TOPICS]
        try:
            await admin.create_topics(topics)
        except TopicAlreadyExistsError:
            pass
    finally:
        await admin.close()


def main(bootstrap_servers: str) -> None:
    asyncio.run(create_topics(bootstrap_servers))
