import asyncio
import json
import logging

from apps.api.db.repository import PostgresRepository
from apps.api.events.kafka_producer import KafkaEventProducer

logger = logging.getLogger(__name__)


class OutboxPublisher:
    def __init__(
        self,
        repository: PostgresRepository,
        producer: KafkaEventProducer,
        poll_interval: float = 0.5,
    ) -> None:
        self._repository = repository
        self._producer = producer
        self._poll_interval = poll_interval
        self._stopped = asyncio.Event()

    async def run(self) -> None:
        while not self._stopped.is_set():
            try:
                await self.publish_batch()
            except Exception:
                logger.exception("outbox publish batch failed")
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=self._poll_interval)
            except TimeoutError:
                pass

    async def publish_batch(self, limit: int = 100) -> int:
        pool = self._repository._require_pool()
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT id, topic, key, payload
                FROM outbox_events
                WHERE published = FALSE
                ORDER BY created_at
                LIMIT $1
                FOR UPDATE SKIP LOCKED
                """,
                limit,
            )
            published = 0
            for row in rows:
                try:
                    payload = row["payload"]
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    await self._producer.publish_raw(row["topic"], row["key"], payload)
                    await connection.execute(
                        """
                        UPDATE outbox_events
                        SET published=TRUE, attempts=attempts+1, updated_at=NOW(), last_error=NULL
                        WHERE id=$1
                        """,
                        row["id"],
                    )
                    published += 1
                except Exception as exc:
                    await connection.execute(
                        """
                        UPDATE outbox_events
                        SET attempts=attempts+1, last_error=$2, updated_at=NOW()
                        WHERE id=$1
                        """,
                        row["id"],
                        str(exc),
                    )
            return published

    def stop(self) -> None:
        self._stopped.set()

