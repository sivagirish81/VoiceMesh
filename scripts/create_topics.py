import asyncio

from apps.api.config import get_settings
from apps.api.events.kafka_admin import create_topics

if __name__ == "__main__":
    asyncio.run(create_topics(get_settings().kafka_bootstrap_servers))
