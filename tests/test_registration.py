import json
import time
import uuid

import pytest

from framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer

def get_activation_token_by_email(email: str, response) -> str:
    items = response.json().get("items") or []
    if not items:
        raise AssertionError("Email not found")
    for item in items:
        recipients = item.get("To") or []
        for recipient in recipients:
            mailbox = recipient.get("Mailbox")
            domain = recipient.get("Domain")
            if mailbox and domain and f"{mailbox}@{domain}" == email:
                user_data = json.loads(item["Content"]["Body"])
                token = user_data["ConfirmationLinkUrl"].split("/")[-1]
                return token
    raise AssertionError("Activation token not found")

@pytest.fixture
def register_message() -> dict[str, str]:
    base = uuid.uuid4().hex
    return {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123123123",
    }


def test_failed_registration(account: AccountApi, mail: MailApi) -> None:
    expected_mail = "string@mail.ru"
    account.register_user(login="string", email=expected_mail, password="string")
    for _ in range (10):
        response = mail.find_message(query=expected_mail)
        if response.json()["total"] > 0:
            break
        time.sleep(1)


def test_success_registration(
    register_events_subscriber: RegisterEventsSubscriber,
    register_message: dict[str, str],
    account: AccountApi,
    mail: MailApi,
) -> None:
    login = register_message["login"]
    account.register_user(**register_message)
    register_events_subscriber.find_message(login=login)

    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_success_registration_with_kafka_producer(
        kafka_producer: Producer,
        mail: MailApi,

) -> None:
    base = uuid.uuid4().hex
    message = {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123123123"
    }
    kafka_producer.send('register-events', message)
    for i in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")

def test_success_registration_with_kafka_producer_consumer(
        register_events_subscriber: RegisterEventsSubscriber,
        kafka_producer: Producer
) -> None:
    base = uuid.uuid4().hex
    message = {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123123123"
    }
    kafka_producer.send('register-events', message)
    for i in range(10):
        message = register_events_subscriber.get_message()
        if message.value["login"] == base:
            break
    else:
        raise AssertionError("Email not found")


def test_register_events_error_consumer(
    account: AccountApi,
    mail: MailApi,
    kafka_producer: Producer
) -> None:
    base = uuid.uuid4().hex
    email = f"{base}@mail.ru"
    message = {
        "input_data": {
            "login": base,
            "email": email,
            "password": "123123123",
        },
        "error_message": {
            "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
            "title": "Validation failed",
            "status": 400,
            "traceId": "00-2bd2ede7c3e4dcf40c4b7a62ac23f448-839ff284720ea656-01",
            "errors": {
                "Email": ["Invalid"],
            },
        },
        "error_type": "unknown",
    }

    kafka_producer.send("register-events-errors", message)

    for _ in range(10):
        response = mail.find_message(query=email)
        response_json = response.json()
        if response_json.get("total", 0) > 0:
            token = get_activation_token_by_email(email=email, response=response)
            activate_response = account.activate_user(token=token)
            assert activate_response.status_code == 200, activate_response.text
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")
