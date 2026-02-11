from framework.internal.kafka.subscriber import Subscriber


class RegisterEventsSubscriber(Subscriber):
    topic: str = "register-events"
