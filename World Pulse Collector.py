#!/usr/bin/env python3
"""
World Pulse Collector: RSS/Atom feeds -> data.json + history.json.
Uses only the Python standard library (Python 3.9+).
Run: python3 collector.py
Env: OUT_DIR (default ".")
"""

import html
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

OUT = os.environ.get("OUT_DIR", ".")
MAX_AGE_H = 24
PER_SOURCE = 45
HIST_POINTS = 48

GN = "https://news.google.com/rss/search?q=site:{}+world+when:1d&hl=en-US&gl=US&ceid=US:en"

SOURCES = {
    # Reuters and AP are fetched via Google News RSS proxy for reliability
    "BBC": "https://feeds.bbci.co.uk/news/world/rss.xml",
    "Al Jazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "DW": "https://rss.dw.com/rdf/rss-en-world",
    "France 24": "https://www.france24.com/en/rss",
    "The Guardian": "https://www.theguardian.com/world/rss",
    "NPR": "https://feeds.npr.org/1004/rss.xml",
    "Sky News": "https://feeds.skynews.com/feeds/rss/world.xml",
    "Reuters": GN.format("reuters.com"),
    "AP": GN.format("apnews.com"),
}
ORDER = list(SOURCES.keys())

# Tension lexicon: word (or prefix*) -> weight (negative = calming / positive news)
LEX = {
    # High tension (+3)
    "war": 3, "wars": 3, "kill*": 3, "dead": 3, "massacre*": 3,
    "invasion": 3, "invad*": 3, "bomb*": 3, "airstrike*": 3,
    "hostage*": 3, "missile*": 3, "terror*": 3, "coup": 3,
    "casualt*": 3, "escalat*": 3, "assassinat*": 3, "genocide": 3,
    # Medium tension (+2)
    "death*": 2, "attack*": 2, "shell*": 2, "nuclear": 2,
    "explosion*": 2, "shooting*": 2, "strike*": 2, "troops": 2,
    "offensive": 2, "clash*": 2, "crisis": 2, "sanction*": 2,
    "threat*": 2, "tension*": 2, "unrest": 2, "blockade": 2,
    "evacuat*": 2, "drone*": 2, "collapse*": 2, "famine": 2,
    "earthquake*": 2, "tsunami": 2, "crackdown": 2, "ultimatum": 2,
    "cyberattack*": 2, "rocket*": 2, "gunmen": 2, "riot*": 2,
    "artillery": 2, "ambush*": 2, "outbreak": 2,
    # Low tension (+1)
    "protest*": 1, "flood*": 1, "wildfire*": 1, "hurricane*": 1,
    "arrest*": 1, "warn*": 1, "recession": 1, "inflation": 1,
    "refugee*": 1, "dispute*": 1, "deficit": 1, "tariff*": 1,
    "detain*": 1, "injured": 1, "wound*": 1,
    # Calming / De-escalation (-1, -2)
    "talks": -1, "negotiat*": -1, "agreement": -2, "agree*": -1,
    "ceasefire": -2, "truce": -2, "peace*": -2, "deal": -1,
    "summit": -1, "aid": -1, "rescue*": -1, "breakthrough": -2,
    "released": -1, "reopen*": -1, "recover*": -1, "cooperat*": -2,
    "treaty": -2, "welcome*": -1, "boost*": -1, "growth": -1,
    "vaccine*": -1, "discover*": -1, "wins": -1, "celebrat*": -1,
    "restor*": -1, "alliance": -1, "humanitarian": -1, "relief": -1,
}

EX = {k: v for k, v in LEX.items() if not k.endswith("*")}
PRE = [(k[:-1], v) for k, v in LEX.items() if k.endswith("*")]

REGIONS = {
    "Европа": (
        "ukraine russia europe european germany france poland britain uk london "
        "paris berlin moscow kyiv kremlin nato brussels italy spain serbia balkans "
        "hungary romania czech sweden norway finland greece eu zelensky putin "
        "moldova belarus baltic"
    ).split(),
    "Ближний Восток": (
        "israel gaza iran iraq syria lebanon yemen saudi hamas hezbollah houthi "
        "houthis tehran beirut qatar uae jordan turkey palestinian palestinians "
        "idf netanyahu jerusalem red sea"
    ).split() + ["west bank", "tel aviv"],
    "Азия": (
        "china japan korea india pakistan taiwan beijing tokyo myanmar thailand "
        "vietnam philippines indonesia afghanistan asia bangladesh nepal "
        "kazakhstan seoul pyongyang manila modi xi"
    ).split() + ["hong kong", "south china sea", "north korea", "south korea"],
    "Африка": (
        "africa sudan nigeria ethiopia congo somalia sahel mali niger kenya "
        "egypt libya ghana uganda mozambique cameroon chad rwanda zimbabwe "
        "senegal algeria morocco tunisia"
    ).split() + ["south africa", "burkina faso"],
    "Америка": (
        "us u.s. usa america american trump biden washington canada mexico brazil "
        "argentina venezuela colombia cuba congress pentagon haiti chile peru "
        "ecuador bolivia ottawa"
    ).split() + ["white house", "wall street", "latin america", "new york"],
}

CATS = {
    "Конфликты": (
        "war attack strike missile troops killed bomb ceasefire hostage military "
        "army drone shelling terror clash invasion offensive soldier rebel "
        "airstrike casualties artillery"
    ).split(),
    "Природа": (
        "earthquake flood wildfire hurricane storm typhoon volcano climate "
        "heatwave drought tsunami cyclone blizzard landslide tornado"
    ).split(),
    "Наука": (
        "scientists scientist study research space nasa vaccine cancer discovery "
        "telescope robot ai technology satellite quantum medical"
    ).split(),
    "Экономика": (
        "economy economic market markets stocks inflation trade tariff tariffs "
        "oil bank gdp currency recession prices shares budget bitcoin dollar "
        "jobs export import debt central bank"
    ).split(),
    "Политика": (
        "election president minister parliament vote government sanctions summit "
        "talks treaty court trial protest prime diplomat opposition lawbill"
    ).split(),
}

STOP = set((
    "the an of to in on for and with as at by from is are was were be been "
    "after over amid says say new news live update updates will would could "
    "may that this its it into than more about out up down against his her "
    "their he she they who what how why when has have had not but all one "
    "two three firstirst"
).split())


def rx(words):
    """Compile case-insensitive word-boundary regex for a list of keywords."""
    pattern = "|".join(re.escape(w) for w in words)
    return re.compile(rf"(?<!\w)(?:{pattern})(?!\w)", re.I)


REG_RX = {k: rx(v) for k, v in REGIONS.items()}
CAT_RX = {k: rx(v) for k, v in CATS.items()}


def tension(text):
    """Calculate headline tension score (0 to 100) using lexicon weights."""
    s = 0
    for w in re.findall(r"[a-z]+", text.lower()):
        if w in EX:
            s += EX[w]
        else:
            for p, wt in PRE:
                if w.startswith(p):
                    s += wt
                    break
    # Base 38 + 9 per net weight point, clamped to [0, 100]
    return max(0, min(100, round(38 + 9 * s)))


def best(rxs, text, default):
    """Find category or region with the highest keyword match count."""
    sc = {k: len(r.findall(text)) for k, r in rxs.items()}
    k = max(sc, key=sc.get)
    return k if sc[k] > 0 else default


def tokens(t):
    """Extract significant alphanumeric tokens for clustering."""
    return {
        w for w in re.findall(r"[a-z0-9]+", t.lower())
        if len(w) > 2 and w not in STOP
    }


def clean(s):
    """Strip HTML tags, decode entities, and normalize whitespace."""
    if not s:
        return ""
    no_tags = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", html.unescape(no_tags)).strip()


def local(tag):
    """Strip XML namespace from tag name."""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def parse_date(s):
    """Parse RFC-2822 or ISO-8601 date string into a UTC timestamp."""
    if not s:
        return time.time()
    s = s.strip()
    try:
        d = parsedate_to_datetime(s)
    except Exception:
        try:
            # Handle ISO format with Z or timezone offset
            iso_s = re.sub(r"Z$", "+00:00", s)
            d = datetime.fromisoformat(iso_s)
        except Exception:
            return time.time()
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return min(time.time(), d.timestamp())


def parse(raw, name):
    """Parse RSS/RDF/Atom XML bytes into normalized article dictionaries."""
    root = ET.fromstring(raw)
    items = []
    gn = name in ("Reuters", "AP")

    for el in root.iter():
        if local(el.tag) not in ("item", "entry"):
            continue
        f = {}
        for c in el:
            k = local(c.tag)
            if k == "link":
                href = c.get("href") or (c.text or "").strip()
                rel = c.get("rel", "alternate")
                if href and (rel == "alternate" or "link" not in f):
                    f["link"] = href
            elif c.text and k not in f:
                f[k] = c.text

        title = clean(f.get("title"))
        if gn:
            # Strip trailing " - Reuters" or " - The Associated Press"
            title = re.sub(r"\s+[-–—]\s+[^-–—]{2,40}$", "", title).strip()
        if not title or len(title) < 12:
            continue

        desc = "" if gn else clean(
            f.get("description") or f.get("summary") or f.get("content")
        )
        # Avoid repeating the title inside description
        if desc.startswith(title):
            desc = desc[len(title):].lstrip(" -–—:.")

        ts = parse_date(
            f.get("pubDate") or f.get("published") or f.get("updated") or f.get("date") or ""
        )
        items.append({
            "src": name,
            "title": title,
            "desc": desc,
            "url": (f.get("link") or "").strip(),
            "ts": ts,
        })

    cutoff = time.time() - MAX_AGE_H * 3600
    items = [i for i in items if i["ts"] >= cutoff]
    items.sort(key=lambda i: -i["ts"])
    return items[:PER_SOURCE]


def fetch(name):
    """Fetch a single feed with retries and fallback SSL context."""
    err = None
    ctx = ssl.create_default_context()
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; WorldPulseBot/2.0; +https://github.com)",
        "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.8",
    }
    for attempt in range(3):
        try:
            req = urllib.request.Request(SOURCES[name], headers=headers)
            with urllib.request.urlopen(req, timeout=20, context=ctx) as r:
                return name, parse(r.read(), name)
        except Exception as e:
            err = e
            time.sleep(1.5 * (attempt + 1))
    print(f"[warn] {name}: {err}", file=sys.stderr)
    return name, []


def same(a, b):
    """Determine if two token sets refer to the same news event."""
    c = len(a & b)
    if not c:
        return False
    j = c / len(a | b)
    return (c >= 3 and j >= 0.24) or (c >= 2 and j >= 0.48)


def cut(s, n):
    """Truncate text cleanly at word boundaries."""
    if not s or len(s) <= n:
        return s or ""
    return s[:n].rsplit(" ", 1)[0].rstrip(",.;:-–—") + "…"


def build(by_src, history=None, now=None):
    """Aggregate feeds, cluster stories, and compute source & regional indices."""
    now = now or time.time()
    allitems = [i for v in by_src.values() for i in v]
    if len(allitems) < 10:
        raise RuntimeError(f"too few headlines collected ({len(allitems)})")

    for i in allitems:
        i["text"] = i["title"] + " " + i["desc"]
        i["ten"] = tension(i["title"] + " " + cut(i["desc"], 120))
        i["tok"] = tokens(i["title"])
        i["reg"] = best(REG_RX, i["text"], "Мир")

    # Story clustering (greedy union by recency)
    clusters = []
    for it in sorted(allitems, key=lambda i: -i["ts"]):
        for c in clusters:
            if same(it["tok"], c["tok"]):
                c["m"].append(it)
                c["tok"] = c["tok"] | it["tok"]
                break
        else:
            clusters.append({"tok": set(it["tok"]), "m": [it]})

    events = []
    for c in clusters:
        m = c["m"]
        srcs = sorted({i["src"] for i in m}, key=ORDER.index)
        age_h = max(0.0, (now - max(i["ts"] for i in m)) / 3600.0)
        ten = round(sum(i["ten"] for i in m) / len(m))
        # Importance combines source diversity, freshness, and tension
        imp = min(100, round(24 * len(srcs) + 25 * max(0.0, 1.0 - age_h / MAX_AGE_H) + ten / 8))

        rep = min(m, key=lambda i: (ORDER.index(i["src"]), -len(i["desc"])))
        joined = " ".join(i["text"] for i in m)
        other = next(
            (i["title"] for i in m if i is not rep and i["title"] != rep["title"]),
            ""
        )
        summary = rep["desc"] or other or f"Сводка по материалам {', '.join(srcs)}."

        events.append({
            "t": cut(rep["title"], 140),
            "x": cut(summary, 260),
            "r": best(REG_RX, joined, "Мир"),
            "c": best(CAT_RX, joined, "Политика"),
            "imp": imp,
            "ten": ten,
            "ago": max(1, round(age_h * 60)),
            "s": srcs,
            "u": rep["url"] if rep["url"].startswith("http") else "",
        })

    # Prioritize multi-source confirmed events, fill up to 14 items
    multi = sorted((e for e in events if len(e["s"]) > 1), key=lambda e: -e["imp"])[:14]
    if len(multi) < 10:
        rest = sorted((e for e in events if len(e["s"]) == 1), key=lambda e: -e["imp"])
        multi += rest[: 10 - len(multi)]

    for n, e in enumerate(multi, 1):
        e["id"] = n

    # Per-source tension index
    srcv = []
    for n in ORDER:
        its = by_src.get(n) or []
        if its:
            srcv.append({"n": n, "v": round(sum(i["ten"] for i in its) / len(its))})

    media = round(sum(s["v"] for s in srcv) / len(srcv)) if srcv else 50

    # Per-region tension index (always output all 5 core regions)
    regv = []
    for r in REGIONS:
        its = [i for i in allitems if i["reg"] == r]
        if its:
            val = round(sum(i["ten"] for i in its) / len(its))
        else:
            val = max(20, min(80, media - 5))
        regv.append({"n": r, "v": val})

    # Update rolling 24h history (up to 48 half-hour points)
    now_ms = round(now * 1000)
    hist = [
        h for h in (history or [])
        if isinstance(h, dict) and "t" in h and "v" in h and (now_ms - h["t"] < 26 * 3600 * 1000)
    ]
    # Avoid duplicate point if script ran < 5 minutes ago
    if hist and (now_ms - hist[-1]["t"] < 5 * 60 * 1000):
        hist[-1] = {"t": now_ms, "v": media}
    else:
        hist.append({"t": now_ms, "v": media})
    hist = hist[-HIST_POINTS:]

    last_pts = [h["v"] for h in hist[-6:]]
    smooth = round(sum(last_pts) / len(last_pts))
    delta = smooth - media

    return {
        "updated": now_ms,
        "demo": False,
        "total_articles": len(allitems),
        "claude_delta": delta,
        "second_delta": delta,
        "second_label": "Сглаженный индекс",
        "second_note": "среднее за последние 3 часа",
        "sources": srcv,
        "regions": regv,
        "events": multi,
        "history": hist,
    }


def write(name, obj):
    """Atomically write JSON data to disk."""
    os.makedirs(OUT, exist_ok=True)
    p = os.path.join(OUT, name)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, p)


def main():
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=len(SOURCES)) as ex:
        by_src = dict(ex.map(fetch, SOURCES))

    ok = [n for n, v in by_src.items() if v]
    total = sum(len(v) for v in by_src.values())
    print(f"[info] sources ok: {len(ok)}/{len(SOURCES)} ({', '.join(ok)}), articles: {total}")

    hist_path = os.path.join(OUT, "history.json")
    try:
        with open(hist_path, "r", encoding="utf-8") as f:
            history = json.load(f)
    except Exception:
        history = []

    try:
        data = build(by_src, history)
    except RuntimeError as e:
        print(f"[error] {e}; data.json left untouched", file=sys.stderr)
        sys.exit(1)

    write("history.json", data["history"])
    write("data.json", data)
    avg = round(sum(s["v"] for s in data["sources"]) / len(data["sources"]))
    elapsed = round(time.time() - t0, 2)
    print(f"[info] media index: {avg}%, clustered events: {len(data['events'])}, time: {elapsed}s")


if __name__ == "__main__":
    main()