"""smarTrader19 autopost: our own post planner. Publishes each scheduled post to the Facebook Page and the
Instagram account through Meta's Graph API (free, no daily cap below Meta's own: 30 FB reels / 100 IG posts).
Runs in GitHub Actions in the smartrader19-posts repo (.github/workflows/autopost.yml). Stdlib only.

    python planner/autopost.py run           post everything due: wakes up to 13 min early, stages the upload
                                             5 min before the slot, publishes on the minute, then the first comment
    python planner/autopost.py check DATE    posts NOTHING: token, both accounts, every media URL, and one
                                             Instagram container per post (Instagram validates the file).
                                             DATE = an stpost day: 4:00 AM ET that day to 4:00 AM the next
    python planner/autopost.py dry [DATE]    print the queue (ET + UTC) next to what the state says

Queue: queue.json on the `planner-data` branch, written ONLY by the dashboard (dashboard/index.html: the owner
drops the day's spreadsheet in, like Post Planner). Each post: id, at (Eastern wall clock), platforms, kind,
media (+ media_jpg for images: Instagram takes JPEG only), caption, comment. The queue is re-read right before
every publish, so a post deleted or edited in the dashboard up to the last minute is honoured.
State: posted.json + STATUS.md on the `state` branch, one orphan commit force-pushed after every change, so a
crash can't make it post twice. Local testing: QUEUE_FILE=path/to/queue.json skips git.
Secrets (set once by meta_setup.py): META_PAGE_TOKEN (never-expiring Page token), META_PAGE_ID, META_IG_ID.
"""
import datetime as dt
import hashlib, json, os, subprocess, sys, time
import urllib.error, urllib.parse, urllib.request
from pathlib import Path
from zoneinfo import ZoneInfo

V = "v25.0"
API = f"https://graph.facebook.com/{V}"
RUPLOAD = f"https://rupload.facebook.com/video-upload/{V}"
ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent
STAGE_EARLY = 5 * 60      # Instagram container / Facebook reel upload starts this long before the slot
LOOKAHEAD = 13 * 60       # a run takes every slot due within this window (cron wakes at :50 and :55)
GRACE = 90 * 60           # a slot more than 90 min late is skipped as "missed" (no 7 AM news at noon)
COMMENT_TRIES = 12
CI = bool(os.environ.get("GITHUB_ACTIONS"))

TOKEN = os.environ.get("META_PAGE_TOKEN", "").strip()
PAGE = os.environ.get("META_PAGE_ID", "").strip()
IG = os.environ.get("META_IG_ID", "").strip()


def now():
    return dt.datetime.now(dt.timezone.utc)


def iso(t):
    return t.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def scrub(s):
    return str(s).replace(TOKEN, "***") if TOKEN else str(s)


def log(*a):
    print(f"[{now():%H:%M:%S}Z]", *(scrub(x) for x in a), flush=True)


# ---------------------------------------------------------------- Graph API
class MetaError(Exception):
    pass


def call(method, url, params=None, headers=None, timeout=180):
    """Graph API call. The token goes in the body (POST) or query (GET) and is scrubbed from every error."""
    params = dict(params or {}, access_token=TOKEN)
    body = None
    if method == "GET":
        url += "?" + urllib.parse.urlencode(params)
    else:
        body = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            err = json.loads(raw).get("error", {})
        except ValueError:
            err = {"message": raw[:300]}
        msg = err.get("error_user_msg") or err.get("message", "")
        raise MetaError(scrub(f"HTTP {e.code} code {err.get('code', '?')}/{err.get('error_subcode', '-')}: {msg}")) from None
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise MetaError(scrub(f"network: {getattr(e, 'reason', e)}")) from None


def rupload(video_id, file_url):
    """Facebook reel upload phase: Meta fetches the hosted mp4 itself (no bytes leave the runner)."""
    req = urllib.request.Request(f"{RUPLOAD}/{video_id}", data=b"", method="POST",
                                 headers={"Authorization": f"OAuth {TOKEN}", "file_url": file_url})
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            j = json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        raise MetaError(scrub(f"rupload HTTP {e.code}: {e.read().decode(errors='replace')[:300]}")) from None
    if not j.get("success"):
        raise MetaError(scrub(f"rupload: {j}"))


# ---------------------------------------------------------------- queue + state
def fetch_branch_file(branch, name):
    """`name` from the tip of `branch` (None if the branch doesn't exist yet)."""
    r = git("ls-remote", "--exit-code", "origin", f"refs/heads/{branch}", check=False)
    if r.returncode == 2:
        return None
    if r.returncode:
        raise RuntimeError(f"can't reach the {branch} branch: " + r.stderr.strip())
    git("fetch", "-q", "--depth=1", "origin", f"+refs/heads/{branch}:refs/remotes/origin/{branch}")
    return git("show", f"origin/{branch}:{name}").stdout


_QCACHE = [0.0, None]


def load_queue(max_age=15):
    """Every post the dashboard has queued, with its due time in UTC. Re-read from GitHub when older than
    max_age seconds, so dashboard edits made up to the last minute are seen without slowing the posting minute."""
    if os.environ.get("QUEUE_FILE"):
        raw = Path(os.environ["QUEUE_FILE"]).read_text(encoding="utf-8")
    elif _QCACHE[1] is not None and time.time() - _QCACHE[0] < max_age:
        raw = _QCACHE[1]
    else:
        raw = fetch_branch_file("planner-data", "queue.json") or '{"posts": []}'
        _QCACHE[:] = [time.time(), raw]
    items = []
    for p in json.loads(raw).get("posts", []):
        due = dt.datetime.fromisoformat(p["at"]).replace(tzinfo=ET).astimezone(dt.timezone.utc)
        items.append(dict(p, due=due, platforms=p.get("platforms") or ["fb", "ig"]))
    return sorted(items, key=lambda p: (p["due"], p["id"]))


def day_window(date):
    """An stpost day ran 5 AM to 12 AM (4:00 AM ET that day up to 4:00 AM the next); from 2026-10-16 it runs 24/7,
    1 AM to 12 AM (midnight is the previous day's last slot), so the window starts at 00:30."""
    a = dt.datetime.fromisoformat(f"{date}T{'00:30' if date >= '2026-10-16' else '04:00'}").replace(tzinfo=ET)
    return a, a + dt.timedelta(days=1)


def in_day(p, date):
    a, b = day_window(date)
    return a <= p["due"] < b


def key(p, plat):
    return f"{p['id']}/{plat}"


def fingerprint(p):
    """Changes when a day is rebuilt (new media or caption): a staged upload of the old version is redone."""
    return hashlib.sha1((p["media"] + "\n" + p["caption"]).encode()).hexdigest()[:12]


def git(*args, input=None, check=True):
    env = dict(os.environ, GIT_AUTHOR_NAME="autopost", GIT_AUTHOR_EMAIL="noreply@github.com",
               GIT_COMMITTER_NAME="autopost", GIT_COMMITTER_EMAIL="noreply@github.com")
    r = subprocess.run(["git", "-C", str(ROOT), *args], input=input, capture_output=True, text=True, env=env)
    if check and r.returncode:
        raise RuntimeError(f"git {args[0]} failed: {r.stderr.strip()}")
    return r


def load_state():
    """Latest posted.json from the state branch. A missing branch = first run; any other failure aborts the
    run (posting with an empty state could post twice)."""
    state = json.loads(fetch_branch_file("state", "posted.json") or "{}")
    _PUSHED[0] = json.dumps(state, indent=1, sort_keys=True)
    return state


_PUSHED = [None]   # the posted.json last saved to GitHub: unchanged state isn't pushed again


def save_state(state):
    """posted.json + STATUS.md as ONE orphan commit on `state` (the branch never grows). GitHub sometimes
    rejects rapid force-pushes ("remote rejected ... (failed)", Oct 11 2026): retry, and never crash a run
    that has already posted because the log couldn't be saved."""
    data = json.dumps(state, indent=1, sort_keys=True)
    (ROOT / "posted.json").write_text(data, encoding="utf-8")
    if not CI or data == _PUSHED[0]:
        return
    lines = []
    for name, text in (("posted.json", data), ("STATUS.md", status_md(state))):
        sha = git("hash-object", "-w", "--stdin", input=text).stdout.strip()
        lines.append(f"100644 blob {sha}\t{name}")
    tree = git("mktree", input="\n".join(lines) + "\n").stdout.strip()
    commit = git("commit-tree", tree, "-m", f"state {iso(now())}").stdout.strip()
    for attempt in range(5):
        r = git("push", "-q", "-f", "origin", f"{commit}:refs/heads/state", check=False)
        if not r.returncode:
            _PUSHED[0] = data
            return
        log("   state save rejected, retrying:", r.stderr.strip().splitlines()[-1] if r.stderr.strip() else r.returncode)
        time.sleep(4 * (attempt + 1))
    log("!! couldn't save the posting log to GitHub (5 tries); the next run retries it")


def status_md(state):
    items = load_queue()
    rows = ["# smarTrader19 autopost status", "", f"Updated {iso(now())}. Times are Eastern. The dashboard shows the same.", "",
            "| Date | Time | Caption | Facebook | Instagram |", "|---|---|---|---|---|"]
    def cell(p, plat):
        if plat not in p["platforms"]:
            return "-"
        s = state.get(key(p, plat), {})
        st = s.get("status", "scheduled")
        if st == "posted":
            c = f"[posted]({s['url']})" if s.get("url") else "posted"
            return c + (" (comment failed)" if s.get("comment") == "failed" else "")
        return st + (f": {s['err'][:80]}" if s.get("err") and st != "scheduled" else "")
    for p in sorted(items, key=lambda p: p["due"], reverse=True)[:80]:
        when = dt.datetime.fromisoformat(p["at"])
        cap = (p["caption"].splitlines() or [""])[0].replace("|", "/")[:60]
        rows.append(f"| {when:%Y-%m-%d} | {when.strftime('%I:%M %p').lstrip('0')} | {cap} | {cell(p, 'fb')} | {cell(p, 'ig')} |")
    return "\n".join(rows) + "\n"


# ---------------------------------------------------------------- posting
def stage(p, plat, s):
    """Before the slot: create the Instagram container (Instagram fetches + processes the file) or upload the
    Facebook reel. Nothing is visible yet. Facebook photos publish in one call, so they have no stage."""
    if plat == "fb" and p["kind"] == "image":
        return True
    if s.get("status") == "staged" and s.get("fp") == fingerprint(p):
        return True
    try:
        if plat == "ig":
            if p["kind"] == "reel":
                r = call("POST", f"{API}/{IG}/media", dict(media_type="REELS", video_url=p["media"], caption=p["caption"]))
            else:
                r = call("POST", f"{API}/{IG}/media", dict(image_url=p["media_jpg"], caption=p["caption"]))
            s.update(status="staged", container=r["id"])
        else:
            vid = call("POST", f"{API}/{PAGE}/video_reels", dict(upload_phase="start"))["video_id"]
            rupload(vid, p["media"])
            s.update(status="staged", video_id=vid)
        s.update(fp=fingerprint(p), staged_at=iso(now()))
        s.pop("err", None)
        log("staged", key(p, plat))
        return True
    except MetaError as e:
        s.update(err=str(e), tries=s.get("tries", 0) + 1)
        log("!! stage failed", key(p, plat), e)
        return False


def wait_container(cid, limit=420):
    """Poll an Instagram container until FINISHED (reels take ~30-90 s). Returns (ok, detail)."""
    t0 = time.time()
    while True:
        r = call("GET", f"{API}/{cid}", dict(fields="status_code,status"))
        code = r.get("status_code")
        if code in ("FINISHED", "PUBLISHED"):
            return True, code
        if code in ("ERROR", "EXPIRED"):
            return False, f"{code}: {r.get('status', '')}"
        if time.time() - t0 > limit:
            return False, f"still {code} after {limit}s"
        time.sleep(8)


def already_posted(plat, caption):
    """Safety net against double posts if state was lost: look for the same caption among the latest posts."""
    want = caption.strip()
    try:
        if plat == "ig":
            for m in call("GET", f"{API}/{IG}/media", dict(fields="id,caption,permalink", limit=15)).get("data", []):
                if (m.get("caption") or "").strip() == want:
                    return m["id"], m.get("permalink")
        else:
            for m in call("GET", f"{API}/{PAGE}/posts", dict(fields="id,message,permalink_url", limit=15)).get("data", []):
                if (m.get("message") or "").strip() == want:
                    return m["id"], m.get("permalink_url")
    except MetaError as e:
        log("   (duplicate check skipped:", e, ")")
    return None


def publish(p, plat, s):
    k = key(p, plat)
    dup = already_posted(plat, p["caption"])
    if dup:
        s.update(status="posted", id=dup[0], url=dup[1], at=iso(now()), comment=s.get("comment", "pending" if p.get("comment") else "none"))
        log("already live, not posting again:", k)
        return True
    if not stage(p, plat, s):
        return False
    try:
        if plat == "ig":
            ok, detail = wait_container(s["container"])
            if not ok:
                s.update(status="error", err=f"Instagram processing {detail}", tries=s.get("tries", 0) + 1)
                s.pop("container", None)            # next attempt makes a fresh container
                log("!!", k, s["err"])
                return False
            mid = call("POST", f"{API}/{IG}/media_publish", dict(creation_id=s["container"]))["id"]
            try:
                url = call("GET", f"{API}/{mid}", dict(fields="permalink")).get("permalink")
            except MetaError:
                url = None
            s.update(status="posted", id=mid, url=url)
        elif p["kind"] == "reel":
            call("POST", f"{API}/{PAGE}/video_reels", dict(upload_phase="finish", video_id=s["video_id"],
                                                          video_state="PUBLISHED", description=p["caption"]))
            s.update(status="posted", id=s["video_id"], url=f"https://www.facebook.com/reel/{s['video_id']}")
        else:
            r = call("POST", f"{API}/{PAGE}/photos", dict(url=p["media"], caption=p["caption"]))
            pid = r.get("post_id") or r["id"]
            s.update(status="posted", id=pid, url=f"https://www.facebook.com/{pid}")
        s.update(at=iso(now()), comment="pending" if p.get("comment") else "none")
        s.pop("err", None)
        log("POSTED", k, s.get("url") or "")
        return True
    except MetaError as e:
        s.update(status="error", err=str(e), tries=s.get("tries", 0) + 1)
        if plat == "fb" and p["kind"] == "reel":
            s.pop("video_id", None)                 # restart the upload next time
        log("!! publish failed", k, e)
        return False


def comment(p, plat, s):
    """First comment as the Page / the account. Facebook reels need a minute of processing first: retried."""
    if s.get("status") != "posted" or s.get("comment") != "pending":
        return
    targets = [s["id"]] if plat == "ig" or p["kind"] == "image" else [s["id"], f"{PAGE}_{s['id']}"]
    err = None
    for t in targets:
        try:
            call("POST", f"{API}/{t}/comments", dict(message=p["comment"]))
            s["comment"] = "done"
            s.pop("cerr", None)
            log("commented", key(p, plat))
            return
        except MetaError as e:
            err = e
    s["ctries"] = s.get("ctries", 0) + 1
    s["cerr"] = str(err)
    if s["ctries"] >= COMMENT_TRIES:
        s["comment"] = "failed"
        log("!! first comment gave up", key(p, plat), err)


def sleep_until(t):
    wait = (t - now()).total_seconds()
    if wait > 0:
        log(f"waiting {wait:.0f}s until {iso(t)}")
        time.sleep(wait)


def need_creds():
    if not (TOKEN and PAGE and IG):
        print("autopost is not connected to Meta yet (secrets META_PAGE_TOKEN / META_PAGE_ID / META_IG_ID missing).")
        print("Run planner/meta_setup.py once on the PC. Nothing was posted.")
        sys.exit(0)


def fresh(p):
    """The same post as the dashboard has it right now (None = deleted there): edits up to the last minute count."""
    return next((q for q in load_queue() if q["id"] == p["id"]), None)


def run():
    need_creds()
    state = load_state()
    items = load_queue()
    t = now()
    todo, changed = [], False
    for p in items:
        late = (t - p["due"]).total_seconds()
        if late > 24 * 3600 or late < -LOOKAHEAD:
            continue
        for plat in p["platforms"]:
            s = state.setdefault(key(p, plat), {})
            if s.get("status") in ("posted", "missed"):
                continue
            if late > GRACE:
                s.update(status="missed", err=s.get("err") or "the runner didn't wake up in time")
                changed = True
                log("missed", key(p, plat))
            else:
                todo.append((p, plat))
    for due in sorted({p["due"] for p, _ in todo}):
        batch = [(p, pl) for p, pl in todo if p["due"] == due]
        sleep_until(due - dt.timedelta(seconds=STAGE_EARLY))
        for p, pl in batch:
            q = fresh(p)
            if q and q["due"] == due and pl in q["platforms"]:
                stage(q, pl, state[key(q, pl)])
        save_state(state)
        sleep_until(due)
        for p, pl in sorted(batch, key=lambda x: x[1]):      # fb first (instant), then ig (waits for processing)
            q = fresh(p)
            if not q or q["due"] != due or pl not in q["platforms"]:
                log("skipped (deleted, moved or unticked in the dashboard):", key(p, pl))
                state[key(p, pl)].pop("err", None)
                continue
            publish(q, pl, state[key(q, pl)])
            save_state(state)
        changed = True
    # first comments: this run's posts plus any still pending from earlier runs
    pending = lambda: [(p, pl) for p in items for pl in p["platforms"]
                       if state.get(key(p, pl), {}).get("comment") == "pending"]
    end = now() + dt.timedelta(minutes=6)
    while pending() and now() < end:
        time.sleep(45)
        for p, pl in pending():
            comment(p, pl, state[key(p, pl)])
        save_state(state)
        changed = True
    if changed:
        save_state(state)
    recent = {key(p, pl) for p in items for pl in p["platforms"] if (t - p["due"]).total_seconds() < 24 * 3600}
    bad = [k for k in recent if state.get(k, {}).get("status") == "error"]
    if bad:
        print("!! failed this run (will retry for 90 min after the slot):", ", ".join(sorted(bad)))
        sys.exit(1)                                     # a red run = GitHub emails the owner


def label(p):
    return f"{p['at'][5:16].replace('T', ' ')} ET"


def check(date):
    """Dry rehearsal for an stpost day: nothing is published."""
    need_creds()
    page = call("GET", f"{API}/{PAGE}", dict(fields="name"))
    acct = call("GET", f"{API}/{IG}", dict(fields="username"))
    print(f"Facebook Page: {page.get('name')}   Instagram: @{acct.get('username')}")
    try:
        q = call("GET", f"{API}/{IG}/content_publishing_limit", dict(fields="config,quota_usage"))["data"][0]
        print(f"Instagram API posts used in the last 24 h: {q.get('quota_usage')} of {q.get('config', {}).get('quota_total')}")
    except (MetaError, KeyError, IndexError) as e:
        print("(publishing limit not readable:", e, ")")
    items = [p for p in load_queue() if in_day(p, date)]
    if not items:
        sys.exit(f"nothing queued for {date}: drop that day's spreadsheet into the dashboard first")
    print(f"{len(items)} posts queued for {date}")
    bad = 0
    for p in items:
        for u in filter(None, (p["media"], p.get("media_jpg"))):
            try:
                with urllib.request.urlopen(urllib.request.Request(u, method="HEAD"), timeout=30) as r:
                    ok = r.status == 200
                    detail = r.headers.get("content-type")
            except urllib.error.HTTPError as e:
                ok, detail = False, f"HTTP {e.code}"
            bad += not ok
            print(f"   {'OK' if ok else '!!'} {detail:<12} {u.rsplit('/', 1)[1]}")
    conts = []
    for p in items:
        if "ig" not in p["platforms"]:
            continue
        try:
            if p["kind"] == "reel":
                r = call("POST", f"{API}/{IG}/media", dict(media_type="REELS", video_url=p["media"], caption=p["caption"]))
            else:
                r = call("POST", f"{API}/{IG}/media", dict(image_url=p["media_jpg"], caption=p["caption"]))
            conts.append((p, r["id"]))
        except MetaError as e:
            bad += 1
            print(f"   !! {label(p)} Instagram refused the container: {e}")
    for p, cid in conts:
        ok, detail = wait_container(cid)
        bad += not ok
        print(f"   {'OK' if ok else '!!'} {label(p)} Instagram {p['kind']}: {detail}")
    print("check passed: every post is ready" if not bad else f"!! {bad} problem(s)")
    sys.exit(1 if bad else 0)


def dry(date=None):
    state = {}
    try:
        state = load_state()
    except Exception as e:
        print("(no state:", e, ")")
    for p in load_queue():
        if date and not in_day(p, date):
            continue
        st = lambda pl: state.get(key(p, pl), {}).get("status", "scheduled") if pl in p["platforms"] else "-"
        print(f"{label(p)}  UTC {p['due']:%m-%d %H:%M}  {p['kind']:<5}  fb={st('fb'):<9} ig={st('ig'):<9} "
              f"{p['caption'].splitlines()[0][:50]}")


if __name__ == "__main__":
    a = sys.argv[1:] or ["run"]
    if a[0] == "run":
        run()
    elif a[0] == "check":
        check(a[1])
    elif a[0] == "dry":
        dry(a[1] if len(a) > 1 and a[1] else None)
    else:
        sys.exit(__doc__)
