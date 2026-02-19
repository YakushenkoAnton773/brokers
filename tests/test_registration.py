import json
import time
import uuid

import pytest

from framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from framework.helpers.kafka.consumers.register_events_error import (
    RegisterEventsErrorSubscriber,
)
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

@pytest.fixture
def invalid_register_message() -> dict[str, str]:
    base = uuid.uuid4().hex
    return {
        "login": base,
        "email": "invalid-email",
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
    register_events_subscriber.find_message(login)

    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_success_registration_with_kafka_producer(
        register_message: dict[str, str],
        kafka_producer: Producer,
        mail: MailApi,

) -> None:
    login = register_message["login"]
    kafka_producer.send('register-events', register_message)
    for i in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")

def test_success_registration_with_kafka_producer_consumer(
        register_message: dict[str, str],
        register_events_subscriber: RegisterEventsSubscriber,
        kafka_producer: Producer
) -> None:
    login = register_message["login"]
    kafka_producer.send('register-events', register_message)
    register_events_subscriber.find_message(login)


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


def test_async_registration_with_invalid_data_validation_error(
    account: AccountApi,
    invalid_register_message: dict[str, str],
    register_events_subscriber: RegisterEventsSubscriber,
    register_events_error_subscriber: RegisterEventsErrorSubscriber,
) -> None:
    login = invalid_register_message["login"]
    account.register_user(**invalid_register_message)

    message = register_events_subscriber.get_message()
    assert message.value.get("login") == login

    message_from_error = register_events_error_subscriber.find_message(
        invalid_register_message["email"]
    )
    assert message_from_error.value.get("input_data", {}).get("login") == login


def test_register_events_unknown_error_is_republished_as_validation(
    kafka_producer: Producer,
    register_events_error_subscriber: RegisterEventsErrorSubscriber,
) -> None:
    incorrect_message = {
        'input_data': {
            'login': '2',
            'email': '2@2',
            'password': '2'
        },
        'error_message': {
            'type': 'https://tools.ietf.org/html/rfc7231#section-6.5.1',
            'title': 'Validation failed',
            'status': 400,
            'traceId': '00-71e7897b8c9979e51ecebf7099ce41c0-3e1a02fb9e5eb0a0-01',
            'errors': {
                'Login': ['Short'],
                'Password': ['Short']
            }
        },
        'error_type': 'unknown'
    }

    message_incorrect_value = incorrect_message
    message_incorrect_value_input_data = message_incorrect_value["input_data"]
    login_message_incorrect = message_incorrect_value_input_data["login"]
    email_message_incorrect = message_incorrect_value_input_data["email"]
    error_type_message_incorrect = message_incorrect_value["error_type"]
    assert error_type_message_incorrect == 'unknown', "Ошибка в тестовых данных, error_type не unknown"

    kafka_producer.send(register_events_error_subscriber.topic, incorrect_message)

    for i in range(2):
        message_from_topic_error = register_events_error_subscriber.find_message(email_message_incorrect)
        message_from_topic_error_value = message_from_topic_error.value
        message_from_topic_error_value_input_data = message_from_topic_error_value["input_data"]
        login = message_from_topic_error_value_input_data["login"]
        email = message_from_topic_error_value_input_data["email"]
        error_type = message_from_topic_error_value["error_type"]

        if login != login_message_incorrect or email != email_message_incorrect:
            raise AssertionError("В топике register-events-errors: не совпали логин или емайл ")

        if i == 0:
            assert error_type == 'unknown', f"В register-events-errors на шаге 0: error_type не unknown, step {i}, {error_type}"
        if i == 1:
            assert error_type == 'validation', f"В register-events-errors на шаге 1: error_type не validation, step {i}, {error_type}"