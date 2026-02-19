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
        except queue.Empty:
            raise AssertionError(
                f"No messages from topic: {self.topic}, within timeout: {timeout}"
            )

    def find_message(
            self,
            find_str: str = None):
        assert find_str != None, "Задайте строку для поиска в сообщении"
        while True:
            if find_str != None:
                message = self.get_message()
                if find_str in str(message):
                    return message