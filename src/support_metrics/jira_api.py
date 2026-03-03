from __future__ import annotations
import time
import requests
from requests.auth import HTTPBasicAuth

class JiraClient:
    def __init__(self, server: str, username: str, token: str, timeout: int = 30):
        self.base = server.rstrip("/")
        self.auth = HTTPBasicAuth(username, token)
        self.timeout = timeout
        self.session = requests.Session()

    def _request(self, method: str, url: str, *, params=None, json=None, attempt_max: int = 8):
        attempt = 0
        while True:
            r = self.session.request(
                method,
                url,
                auth=self.auth,
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                params=params,
                json=json,
                timeout=self.timeout,
            )
            if r.status_code != 429:
                r.raise_for_status()
                return r.json()

            attempt += 1
            if attempt >= attempt_max:
                raise RuntimeError("Rate limit: too many 429 responses")

            retry_after = r.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                time.sleep(int(retry_after))
            else:
                time.sleep(min(60, 2 ** attempt))

    def search_issues(self, jql: str, fields: list[str], next_page_token: str | None = None, max_results: int = 100):
        url = f"{self.base}/rest/api/3/search/jql"
        payload = {"jql": jql, "fields": fields, "maxResults": max_results}
        if next_page_token:
            payload["nextPageToken"] = next_page_token
        return self._request("POST", url, json=payload)

    def get_issue(self, issue_key: str, fields: list[str], expand: str = "changelog"):
        url = f"{self.base}/rest/api/3/issue/{issue_key}"
        params = {"fields": ",".join(fields), "expand": expand}
        return self._request("GET", url, params=params)

    def get_comments(self, issue_key: str, start_at: int = 0, max_results: int = 50):
        url = f"{self.base}/rest/api/3/issue/{issue_key}/comment"
        params = {"startAt": start_at, "maxResults": max_results}
        return self._request("GET", url, params=params)
