#!/usr/bin/env python3
"""
World Pulse Collector 2.0 — RSS/Atom мировых СМИ -> data.json + history.json.
Только стандартная библиотека (Python 3.9+). Запуск:
  python collector.py                 один проход
  python collector.py --loop          бесконечный цикл, каждые 300 сек (5 минут)
  python collector.py --loop --every 120
Переменные окружения: OUT_DIR (папка вывода, по умолчанию "."), TRANSLATE=0 (выключить перевод на русский).
"""
import argparse
import html
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

OUT = os.environ.get("OUT_DIR", ".")
TRANSLATE = os.environ.get("TRANSLATE", "1") != "0"
MAX_AGE_H = 36
PER_SOURCE = 40
HIST_POINTS = 48          # 48 точек по 30 минут = 24 часа
HIST_STEP_MIN = 30
MAX_EVENTS = 16
UA = "Mozilla/5.0 (compatible; WorldPulse/2.0)"

# Надёжные и прямые RSS-потоки ведущих мировых агентств
SOURCES = {
    "BBC": "https://feeds.bbci.co.uk/news/world/rss.xml",
    "Al Jazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "DW": "https://rss.dw.com/rdf/rss-en-world",
    "France 24": "https://www.france24.com/en/rss",
    "The Guardian": "https://www.theguardian.com/world/rss",
    "Sky News": "https://feeds.skynews.com/feeds/rss/world.xml",
    "NPR": "https://feeds.npr.org/1004/rss.xml",
    "CNBC": "https://search.cnbc.com/rs/search/view.html?partnerId=2000&keywords=world&cat=100003114&pub=all",
    "UN News": "https://news.un.org/feed/subscribe/en/news/all/rss.xml"
}
ORDER = list(SOURCES.keys())

# Лексикон напряжённости
LEX = {
    # Высокая напряжённость (+3)
    "war": 3, "wars": 3, "kill*": 3, "dead": 3, "massacre*": 3,
    "invasion": 3, "invad*": 3, "bomb*": 3, "airstrike*": 3,
    "hostage*": 3, "missile*": 3, "terror*": 3, "coup": 3,
    "casualt*": 3, "escalat*": 3, "assassinat*": 3, "genocide": 3,
    # Средняя (+2)
    "death*": 2, "attack*": 2, "shell*": 2, "nuclear": 2,
    "explosion*": 2, "shooting*": 2, "strike*": 2, "troops": 2,
    "offensive": 2, "clash*": 2, "crisis": 2, "sanction*": 2,
    "threat*": 2, "tension*": 2, "unrest": 2, "blockade": 2,
    "evacuat*": 2, "drone*": 2, "collapse*": 2, "famine": 2,
    "earthquake*": 2, "tsunami": 2, "crackdown": 2, "ultimatum": 2,
    "cyberattack*": 2, "rocket*": 2, "gunmen": 2, "riot*": 2,
    # Умеренная (+1)
    "protest*": 1, "flood*": 1, "wildfire*": 1, "hurricane*": 1,
    "arrest*": 1, "warn*": 1, "recession": 1, "inflation": 1,
    "refugee*": 1, "dispute*": 1, "deficit": 1, "tariff*": 1,
    # Деэскалация / Позитив (-1, -2)
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
        "hungary romania czech sweden norway finland greece eu zelensky putin"
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
        "egypt libya ghana uganda mozambique cameroon chad rwanda zimbabwe"
    ).split() + ["south africa", "burkina faso"],
    "Америка": (
        "us u.s. usa america american trump biden washington canada mexico brazil "
        "argentina venezuela colombia cuba congress pentagon haiti chile peru"
    ).split() + ["white house", "wall street", "latin america", "new york"],
}

CATS = {
    "Конфликты": "war attack strike missile troops killed bomb ceasefire hostage military army drone terror clash invasion".split(),
    "Природа": "earthquake flood wildfire hurricane storm typhoon volcano climate drought tsunami".split(),
    "Наука": "scientists scientist study research space nasa vaccine cancer discovery ai robot satellite quantum".split(),
    "Экономика": "economy market stocks inflation trade tariff oil bank gdp recession prices dollar fed interest".split(),
    "Политика": "election president minister parliament vote government sanctions summit talks treaty court protest".split(),
}

STOP = set((
    "the an of to in on for and with as at by from is are was were be been "
    "after over amid says say new news live update updates will would could "
    "may that this its it into than more about out up down against his her "
    "their he she they who what how why when has have had not but all one two"
).split())


SKIP_RX = re.compile(
    r"\b(football|soccer|premier league|champions league|nba|nfl|nhl|mlb|cricket|rugby|tennis|golf|"
    r"formula 1|f1|olympic\w*|transfer window|striker|goalkeeper|celebrity|oscars?|grammys?|"
    r"box office|fashion week|horoscope|recipes?|royal family)\b", re.I)
BREAK_RX = re.compile(r"\b(breaking|urgent|just in|developing)\b", re.I)
IMG_BAD = re.compile(r"(logo|favicon|sprite|pixel|1x1|blank|placeholder|avatar|icon)", re.I)


def rx(words):
    pattern = "|".join(re.escape(w) for w in words)
    return re.compile(rf"(?<!\w)(?:{pattern})(?!\w)", re.I)


REG_RX = {k: rx(v) for k, v in REGIONS.items()}
CAT_RX = {k: rx(v) for k, v in CATS.items()}


def clean(s):
    if not s:
        return ""
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", str(s)))).strip()


def local(tag):
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def tension(text):
    s = 0
    for w in re.findall(r"[a-z]+", text.lower()):
        if w in EX:
            s += EX[w]
        else:
            for p, wt in PRE:
                if w.startswith(p):
                    s += wt
                    break
    return max(15, min(95, round(40 + 8.5 * s)))


def best(rxs, text, default):
    sc = {k: len(r.findall(text)) for k, r in rxs.items()}
    k = max(sc, key=sc.get)
    return k if sc[k] > 0 else default


def tokens(t):
    return {w for w in re.findall(r"[a-z0-9]+", t.lower()) if len(w) > 2 and w not in STOP}


def parse_date(s):
    if not s:
        return time.time()
    try:
        d = parsedate_to_datetime(s.strip())
    except Exception:
        try:
            d = datetime.fromisoformat(re.sub(r"Z$", "+00:00", s.strip()))
        except Exception:
            return time.time()
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return min(time.time(), d.timestamp())


def norm_img(u):
    u = (u or "").strip()
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("http://"):
        u = "https://" + u[7:]
    if not u.startswith("https://") or len(u) > 600 or IMG_BAD.search(u):
        return ""
    return re.sub(r"/standard/\d+/", "/standard/800/", u)  # BBC: крупнее миниатюра


def parse_xml(raw_bytes, name):
    try:
        root = ET.fromstring(raw_bytes)
    except Exception as e:
        print(f"[warn] XML {name}: {e}", file=sys.stderr)
        return []
    items = []
    for el in root.iter():
        if local(el.tag) not in ("item", "entry"):
            continue
        f, raw, imgs = {}, "", []

        def add_media(node):
            u = node.get("url") or node.get("href")
            typ = (node.get("type") or node.get("medium") or "image").lower()
            if u and "image" in typ:
                try:
                    w = int(node.get("width") or 0)
                except ValueError:
                    w = 0
                imgs.append((w, u))

        for c in el:
            k, is_media = local(c.tag), "mrss" in c.tag
            if is_media and k in ("content", "thumbnail"):
                add_media(c)
            elif is_media and k == "group":
                for sub in c:
                    if local(sub.tag) in ("content", "thumbnail"):
                        add_media(sub)
            elif k == "enclosure":
                add_media(c)
            elif k == "link":
                href, rel = c.get("href"), c.get("rel", "alternate")
                if rel == "enclosure":
                    add_media(c)
                elif (href or c.text) and (rel == "alternate" or "link" not in f):
                    f["link"] = (href or c.text).strip()
            elif k in ("description", "summary", "encoded") or (k == "content" and not is_media):
                if c.text:
                    raw += " " + c.text
                    f.setdefault("description", c.text)
            elif c.text and k not in f:
                f[k] = c.text
        title = clean(f.get("title"))
        if len(title) < 12:
            continue
        desc = clean(f.get("description"))
        if desc.startswith(title):
            desc = desc[len(title):].lstrip(" -–—:.")
        img = ""
        if imgs:
            img = norm_img(max(imgs, key=lambda x: x[0])[1])
        if not img:
            m = re.search(r"<img[^>]+src=[\"']([^\"']+)", html.unescape(raw), re.I)
            img = norm_img(m.group(1)) if m else ""
        ts = parse_date(f.get("pubDate") or f.get("published") or f.get("updated") or f.get("date") or "")
        items.append({"src": name, "title": title, "desc": desc[:260], "url": f.get("link", ""), "ts": ts, "img": img})
    cutoff = time.time() - MAX_AGE_H * 3600
    items = [i for i in items if i["ts"] >= cutoff]
    items.sort(key=lambda i: -i["ts"])
    return items[:PER_SOURCE]


def fetch_feed(name, url):
    err = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.8"})
            with urllib.request.urlopen(req, timeout=18) as r:
                return name, parse_xml(r.read(), name)
        except Exception as e:
            err = e
            time.sleep(1.2 * (attempt + 1))
    print(f"[warn] {name}: пропущен ({err})", file=sys.stderr)
    return name, []


def same(a, b):
    c = len(a & b)
    if not c:
        return False
    j = c / len(a | b)
    return (c >= 3 and j >= 0.22) or (c >= 2 and j >= 0.45)


def cut(s, n):
    if len(s) <= n:
        return s
    return s[:n].rsplit(" ", 1)[0].rstrip(",.;:") + "…"


def build_dataset(by_src, history=None, now=None):
    now = now or time.time()
    allitems = [i for v in by_src.values() for i in v if not SKIP_RX.search(i["title"])]
    if len(allitems) < 5:
        raise RuntimeError("слишком мало заголовков")
    for i in allitems:
        i["text"] = i["title"] + " " + i["desc"]
        i["ten"] = tension(i["text"])
        i["tok"] = tokens(i["title"])
        i["reg"] = best(REG_RX, i["text"], "Мир")

    clusters = []
    for it in sorted(allitems, key=lambda i: -i["ts"]):
        for c in clusters:
            if same(it["tok"], c["tok"]):
                c["m"].append(it)
                c["tok"] |= it["tok"]
                break
        else:
            clusters.append({"tok": set(it["tok"]), "m": [it]})

    rank = lambda s: ORDER.index(s) if s in ORDER else 99
    events = []
    for c in clusters:
        m = c["m"]
        srcs = sorted({i["src"] for i in m}, key=rank)
        age_h = max(0.0, (now - max(i["ts"] for i in m)) / 3600)
        ten = round(sum(i["ten"] for i in m) / len(m))
        decay = 0.5 ** (age_h / 3)                      # полураспад свежести — 3 часа
        brk = 0.05 if any(BREAK_RX.search(i["title"]) for i in m) else 0
        imp = min(100, round(100 * (0.45 * min(len(srcs), 4) / 4 + 0.40 * decay + 0.15 * ten / 100 + brk)))
        rep = min(m, key=lambda i: (rank(i["src"]), -len(i["desc"])))
        joined = " ".join(i["text"] for i in m)
        desc = rep["desc"] or next((i["desc"] for i in m if i["desc"]), "")
        pic = rep if rep["img"] else next((i for i in sorted(m, key=lambda i: rank(i["src"])) if i["img"]), None)
        events.append({
            "t": cut(rep["title"], 150), "x": cut(desc, 260),
            "r": best(REG_RX, joined, "Мир"), "c": best(CAT_RX, joined, "Общее"),
            "imp": imp, "ten": ten, "ago": max(1, round(age_h * 60)), "s": srcs,
            "u": rep["url"] if rep["url"].startswith("http") else "",
            "img": pic["img"] if pic else "", "img_src": pic["src"] if pic else "",
            "fresh": age_h <= 1, "n": len(m),
        })
    events.sort(key=lambda e: (-e["imp"], e["ago"]))
    events = events[:MAX_EVENTS]
    for n, e in enumerate(events, 1):
        e["id"] = n
        e["t_en"], e["x_en"] = e["t"], e["x"]

    srcv = []
    for n in ORDER:
        its = [i for i in allitems if i["src"] == n]
        if its:
            srcv.append({"n": n, "v": round(sum(i["ten"] for i in its) / len(its))})
    media = round(sum(s["v"] for s in srcv) / len(srcv))
    regv = []
    for r in REGIONS:
        its = [i for i in allitems if i["reg"] == r]
        if len(its) >= 2:                                # регион без данных не выдумываем
            regv.append({"n": r, "v": round(sum(i["ten"] for i in its) / len(its))})

    now_ms = round(now * 1000)
    hist = [h for h in (history or []) if isinstance(h, dict) and "t" in h and "v" in h and now_ms - h["t"] < 26 * 3600e3]
    if hist and now_ms - hist[-1]["t"] < HIST_STEP_MIN * 60e3:
        hist[-1]["v"] = media                            # текущая точка обновляется на месте
    else:
        hist.append({"t": now_ms, "v": media})
    hist = hist[-HIST_POINTS:]
    last = [h["v"] for h in hist[-6:]]
    smooth = round(sum(last) / len(last))
    return {
        "updated": now_ms, "demo": False, "total_articles": len(allitems),
        "second_delta": smooth - media, "second_label": "Сглаженный индекс",
        "second_note": "среднее за последние 3 часа",
        "sources": srcv, "regions": regv, "events": events, "history": hist,
    }


def load_json(name, default):
    try:
        with open(os.path.join(OUT, name), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(name, obj):
    os.makedirs(OUT, exist_ok=True)
    p = os.path.join(OUT, name)
    with open(p + ".tmp", "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)
    os.replace(p + ".tmp", p)


def gt(text):
    url = "https://translate.googleapis.com/translate_a/single?client=gtx&sl=en&tl=ru&dt=t&q=" + urllib.parse.quote(text[:450])
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}), timeout=10) as r:
        d = json.loads(r.read().decode("utf-8"))
    return "".join(s[0] for s in d[0] if s and s[0]).strip()


def translate_events(events):
    """Перевод на русский (неофициальный endpoint Google, с кэшем). При сбое остаётся английский."""
    if not TRANSLATE:
        return
    cache = load_json("translations.json", {})
    used = {v for e in events for v in (e["t_en"], e["x_en"]) if v}
    need = sorted(used - set(cache))[:40]
    fails = [0]

    def job(t):
        if fails[0] >= 4:
            return
        try:
            cache[t] = gt(t)
        except Exception:
            fails[0] += 1

    if need:
        with ThreadPoolExecutor(max_workers=5) as ex:
            list(ex.map(job, need))
        print(f"[info] переведено {len([t for t in need if t in cache])}/{len(need)}")
    for e in events:
        e["t"] = cache.get(e["t_en"], e["t_en"])
        e["x"] = cache.get(e["x_en"], e["x_en"]) if e["x_en"] else ""
    keep = [k for k in cache if k not in used][-300:] + [k for k in used if k in cache]
    save_json("translations.json", {k: cache[k] for k in keep})


def run_once():
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=len(SOURCES)) as ex:
        by_src = dict(ex.map(lambda kv: fetch_feed(*kv), SOURCES.items()))
    ok = [n for n, v in by_src.items() if v]
    print(f"[info] источников: {len(ok)}/{len(SOURCES)}, заголовков: {sum(len(v) for v in by_src.values())}")
    try:
        data = build_dataset(by_src, load_json("history.json", []))
    except RuntimeError as e:
        print(f"[warn] {e}: data.json не тронут", file=sys.stderr)
        return False
    data["failed"] = [n for n in SOURCES if n not in ok]
    translate_events(data["events"])
    save_json("history.json", data["history"])
    save_json("data.json", data)
    print(f"[done] {len(data['events'])} сюжетов, с картинками: {sum(1 for e in data['events'] if e['img'])}, {round(time.time() - t0, 1)} c")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", action="store_true", help="работать бесконечно")
    ap.add_argument("--every", type=int, default=300, help="пауза между проходами, сек (по умолчанию 300)")
    a = ap.parse_args()
    if not a.loop:
        run_once()
        return
    while True:
        try:
            run_once()
        except Exception as e:
            print(f"[error] {e}", file=sys.stderr)
        time.sleep(a.every)


if __name__ == "__main__":
    main()
