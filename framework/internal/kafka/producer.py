import json
import threading
from types import TracebackType
from kafka import KafkaProducer



class Producer:
    def __init__ (self,bootstrap_servers: list[str] = ["185.185.143.231:9092"]):
        self._bootstrap_servers= bootstrap_servers
        self._producer : KafkaProducer | None = None
        self._lock: threading.Lock = threading.Lock()

    def start(self) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers= self._bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            acks="all",
            retries=5,
            retry_backoff_ms=5000,
            request_timeout_ms=70000,
            connections_max_idle_ms=65000,
            reconnect_backoff_ms=5000,
            reconnect_backoff_max_ms=10000,
        )

    def stop(self)-> None:
        if self._producer:
            self._producer.close()
            self._producer = None

    def send(self, topic:str, message: dict[str,str]) -> None:
        if not self._producer:
            raise RuntimeError("Producer is not started")

        try:
            with self._lock:
                future = self._producer.send(topic, value=message)
                record_metadata = future.get(timeout=10)
                return record_metadata
        except Exception as e:
            raise RuntimeError(f"Failed to send message to Kafka: {e}")


    def __enter__(self) -> "Producer":
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException],
        exc_val: BaseException | None,
        exc_tb: TracebackType | None
    ) -> None:
        self.stop()
