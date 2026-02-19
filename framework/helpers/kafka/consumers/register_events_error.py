from framework.internal.kafka.subscriber import Subscriber


class RegisterEventsErrorSubscriber(Subscriber):
    topic: str = "register-events-errors"