import time
import uuid
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer


def test_failed_registration(account: AccountApi, mail: MailApi) -> None:
    expected_mail = "string@mail.ru"
    account.register_user(login="string", email=expected_mail, password="string")
    for _ in range (10):
        response = mail.find_message(query=expected_mail)
        if response.json()["total"] > 0:
            break
        time.sleep(1)


def test_success_registration(account: AccountApi, mail: MailApi) -> None:
    base = uuid.uuid4().hex
    account.register_user(login=base, email=f"{base}@mail.ru", password="123123123")
    for _ in range (10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
    else:
        raise AssertionError("Email not found")


def test_success_registration_with_kafka_producer(mail: MailApi, kafka_producer: Producer) -> None:
    base = uuid.uuid4().hex
    message = {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123123123"
    }
    kafka_producer.send('register-events', message)
    for _ in range (10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
    else:
        raise AssertionError("Email not found")