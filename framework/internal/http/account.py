import httpx


class AccountApi:
    def __init__(self, client: httpx.Client):
        self._base_url = ""
        self._client = httpx.Client(base_url=self._base_url)

        def register_user(self, lo):
