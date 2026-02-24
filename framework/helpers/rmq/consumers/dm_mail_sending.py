import json
import time

from framework.internal.rmq.consumer import Consumer


class DmMailSending(Consumer):
    exchange = "dm.mail.sending"
    routing_key = "#"


    def find_message(self, login: str, timeout: float = 10.0) -> None:
        start_time = time.time()

        while time.time() - start_time < timeout:
            message = self.get_message(timeout=timeout)
            if json.loads(message['body'])["Login"] == login:
                break
        else:
            raise AssertionError(f"Message for rmq: {self.exchange} not found")