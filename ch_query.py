
import requests, json

class CH:
    def __init__(self, url: str, user: str = "", password: str = "", timeout: int = 10):
        self.url = url.rstrip("/")
        self.auth = (user, password) if user else None
        self.timeout = timeout

    def select_json_each_row(self, sql: str):
        if "FORMAT" not in sql.upper():
            sql = sql.rstrip(";") + " FORMAT JSONEachRow"
        r = requests.post(f"{self.url}/", params={"query": sql}, timeout=self.timeout, auth=self.auth)
        r.raise_for_status()
        lines = [ln for ln in r.text.splitlines() if ln.strip()]
        return [json.loads(ln) for ln in lines]
