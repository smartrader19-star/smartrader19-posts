"""Reads SmarTrader's real follower counts from Meta and saves them as followers.json on the planner-data branch, where
hourly.py picks them up for the "N/100 followers" line (owner, Oct 30 2026: "put the insta followers only for insta and
fb followers only for fb. and you auto update it"). Runs in GitHub Actions (followers.yml), the only place with the
Meta tokens. Stdlib only.
"""
import base64, datetime as dt, json, os, urllib.error, urllib.parse, urllib.request

API = "https://graph.facebook.com/v25.0"
TOKEN, PAGE, IG = (os.environ[k].strip() for k in ("META_PAGE_TOKEN", "META_PAGE_ID", "META_IG_ID"))
REPO, GH = os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_TOKEN"]


def meta(node, field):
    q = urllib.parse.urlencode({"fields": field, "access_token": TOKEN})
    with urllib.request.urlopen(f"{API}/{node}?{q}", timeout=60) as r:
        return int(json.load(r)[field])


def github(method, path, body=None):
    req = urllib.request.Request(f"https://api.github.com/repos/{REPO}/{path}", method=method,
                                 data=json.dumps(body).encode() if body else None,
                                 headers={"Authorization": f"Bearer {GH}", "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


counts = {"ig": meta(IG, "followers_count"), "fb": meta(PAGE, "followers_count"),
          "at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
print(counts)
try:
    sha = github("GET", "contents/followers.json?ref=planner-data")["sha"]
except urllib.error.HTTPError as e:
    if e.code != 404:
        raise
    sha = None
body = {"message": f"followers: ig {counts['ig']}, fb {counts['fb']}", "branch": "planner-data",
        "content": base64.b64encode(json.dumps(counts, indent=1).encode()).decode()}
if sha:
    body["sha"] = sha
github("PUT", "contents/followers.json", body)
