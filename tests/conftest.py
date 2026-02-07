import pytest

from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi


@pytest.fixture(scope="session")
def account() -> AccountApi:
    return AccountApi()

@pytest.fixture(scope="session")
def mail() -> MailApi:
    return MailApi()