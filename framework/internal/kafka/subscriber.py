import queue
import time
from kafka.consumer.fetcher import ConsumerRecord
from abc import (
    ABC,
    abstractmethod,
)


class Subscriber(ABC):
    def __init__(self):
        self._messages: queue.Queue = queue.Queue()

    @property
    @abstractmethod
    def topic(self) -> str: ...

    def handle_message(self, record: ConsumerRecord) -> None:
        self._messages.put(record)

    def get_message(self, timeout: int = 90):
        try:
            return self._messages.get(timeout=timeout)
        except queue.Queue:
            raise AssertionError(
                f"No messages from topic: {self.topic}, within timeout: {timeout}"
            )

    def find_message(
            self,
            login: str,
            timeout: int = 90
            ) -> ConsumerRecord:
        try:
            while True:
                record = self._messages.get(timeout=timeout)
                if record.value.get("login") == login:
                    return record
        except queue.Empty:
            pass

        raise AssertionError(
            f"Message with login '{login}' not found in topic: {self.topic}, within timeout: {timeout}"
        )

