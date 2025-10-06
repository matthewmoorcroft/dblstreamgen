"""Publishers for streaming destinations (Kinesis, Kafka, etc.)."""

from dblstreamgen.publishers.kinesis import KinesisPublisher, PublishResult

__all__ = ["KinesisPublisher", "PublishResult"]

