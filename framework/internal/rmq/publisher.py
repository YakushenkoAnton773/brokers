import json
import uuid
from types import TracebackType
from pika.adapters.blocking_connection import BlockingChannel
import pika
from framework.internal.singleton import Singleton


class RmqPublisher(Singleton):
    def __init__(self, url: str = "amqp://guest:guest@185.185.143.231:5672/"):
        self._url = url
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None

    def _start(self):
        self._connection = pika.BlockingConnection(
            pika.URLParameters(self._url))
        self._channel = self._connection.channel()

    def _stop(self):
        if self._channel is not None:
            self._channel.close()
        if self._connection is not None:
            self._connection.close()


    def __enter__(self) -> "RmqPublisher":
        self._start()
        return self

    def __exit__(
            self,
            exc_type: type[BaseException],
            exc_val: BaseException | None,
            exc_tb: TracebackType | None,
    ) -> None:
        self._stop()


    def publish(
            self,
            exchange: str,
            message: dict[str, str],
            routing_key: str = "",
            properties: pika.BasicProperties  | None = None,
            ):
        if properties is None:
            properties = pika.BasicProperties(
                content_type="application/json",
                correlation_id=str(uuid.uuid4()),
            )

        message = json.dumps(message).encode("utf-8")
        self._channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message,
            properties=properties,
        )

