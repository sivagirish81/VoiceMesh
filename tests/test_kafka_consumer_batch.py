import pytest
from aiokafka.structs import TopicPartition

from apps.api.events.kafka_consumer import ConsumedEvent, KafkaEventConsumer
from apps.api.events.schemas import EventType, PipelineEvent


class FakeAIOKafkaConsumer:
    def __init__(self) -> None:
        self.committed: dict[TopicPartition, int] | None = None

    async def commit(self, offsets: dict[TopicPartition, int]) -> None:
        self.committed = offsets


def make_consumed(offset: int, partition: int = 0) -> ConsumedEvent:
    return ConsumedEvent(
        event=PipelineEvent.create(
            call_id="call-1",
            turn_id="turn-1",
            event_type=EventType.USAGE_STT_RECORDED,
            stage="stt",
            sequence_number=offset + 1,
        ),
        topic="usage-events",
        partition=partition,
        offset=offset,
        headers=[],
    )


@pytest.mark.asyncio
async def test_commit_batch_uses_next_offset_per_partition() -> None:
    consumer = KafkaEventConsumer.__new__(KafkaEventConsumer)
    fake = FakeAIOKafkaConsumer()
    consumer._consumer = fake

    await consumer._commit_batch(
        [
            make_consumed(10, partition=0),
            make_consumed(11, partition=0),
            make_consumed(7, partition=1),
        ]
    )

    assert fake.committed == {
        TopicPartition("usage-events", 0): 12,
        TopicPartition("usage-events", 1): 8,
    }
