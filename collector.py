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
import math
import zlib
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
MAX_EVENTS = 40
UA = "Mozilla/5.0 (compatible; WorldPulse/2.0)"

GN = "https://news.google.com/rss/search?q=site:{}+when:1d&hl=en-US&gl=US&ceid=US:en"
GN_NAMES = {"Reuters", "AP"}   # у Reuters и AP нет публичного RSS, берём через Google News
SOURCES = {
    "BBC": "https://feeds.bbci.co.uk/news/world/rss.xml",
    "Al Jazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "DW": "https://rss.dw.com/rdf/rss-en-world",
    "France 24": "https://www.france24.com/en/rss",
    "The Guardian": "https://www.theguardian.com/world/rss",
    "Sky News": "https://feeds.skynews.com/feeds/rss/world.xml",
    "NPR": "https://feeds.npr.org/1004/rss.xml",
    "UN News": "https://news.un.org/feed/subscribe/en/news/all/rss.xml",
    "CNBC": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362",
    "Euronews": "https://www.euronews.com/rss?format=mrss&level=theme&name=news",
    "NYT": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    "Washington Post": "https://feeds.washingtonpost.com/rss/world",
    "CBS News": "https://www.cbsnews.com/latest/rss/world",
    "ABC News": "https://abcnews.go.com/abcnews/internationalheadlines",
    "CNN": "http://rss.cnn.com/rss/edition_world.rss",
    "Fox News": "https://moxie.foxnews.com/google-publisher/world.xml",
    "ABC Australia": "https://www.abc.net.au/news/feed/2942460/rss.xml",
    "CNA": "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=6511",
    "Times of Israel": "https://www.timesofisrael.com/feed/",
    "Middle East Eye": "https://www.middleeasteye.net/rss",
    "Arab News": "https://www.arabnews.com/rss.xml",
    "Africanews": "https://www.africanews.com/feed/rss",
    "Kyiv Independent": "https://kyivindependent.com/news-archive/rss/",
    "Yonhap": "https://en.yna.co.kr/RSS/news.xml",
    "Nikkei Asia": "https://asia.nikkei.com/rss/feed/nar",
    "The Diplomat": "https://thediplomat.com/feed/",
    "Dawn": "https://www.dawn.com/feeds/home",
    "Times of India": "https://timesofindia.indiatimes.com/rssfeeds/296589292.cms",
    "The Hindu": "https://www.thehindu.com/news/international/feeder/default.rss",
    "Politico EU": "https://www.politico.eu/feed/",
    "Japan Times": "https://www.japantimes.co.jp/feed/",
    "Reuters": GN.format("reuters.com"),
    "AP": GN.format("apnews.com"),
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
        if name in GN_NAMES:
            title = re.sub(r"\s+[-–—|]\s+[^-–—|]{2,40}$", "", title)
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


GAZ_TXT = """Afghanistan|Афганистан|33.9|67.7|afghan,taliban
Albania|Албания|41.2|20|albanian
Algeria|Алжир|28|2.6|algerian,algiers
Angola|Ангола|-12.3|17.5|angolan
Argentina|Аргентина|-35|-65|argentine,argentinian,milei
Armenia|Армения|40.2|45|armenian,yerevan
Australia|Австралия|-25|134|australian,canberra,sydney,melbourne
Austria|Австрия|47.5|14.6|austrian,vienna
Azerbaijan|Азербайджан|40.3|47.7|azerbaijani,baku
Bahrain|Бахрейн|26|50.5|bahraini
Bangladesh|Бангладеш|23.7|90.3|bangladeshi
Belarus|Беларусь|53.7|28|belarusian,lukashenko
Belgium|Бельгия|50.6|4.7|belgian
Bolivia|Боливия|-16.7|-64.7|bolivian
Bosnia|Босния|44|17.8|bosnian,sarajevo
Botswana|Ботсвана|-22.3|24.7|botswana
Brazil|Бразилия|-10|-52|brazilian,lula
Bulgaria|Болгария|42.7|25.3|bulgarian,sofia
Burkina Faso|Буркина-Фасо|12.3|-1.6|burkinabe
Burundi|Бурунди|-3.4|29.9|burundi
Cambodia|Камбоджа|12.7|104.9|cambodian
Cameroon|Камерун|5.7|12.7|cameroonian
Canada|Канада|58|-96|canadian,trudeau,carney
Central African Republic|ЦАР|6.6|20.5|central african
Chad|Чад|15.3|18.7|chadian
Chile|Чили|-33|-71|chilean
China|Китай|35|103|chinese,xi jinping,PLA
Colombia|Колумбия|4.6|-74|colombian
DR Congo|ДР Конго|-2.9|23.6|congo,congolese,DRC
Costa Rica|Коста-Рика|9.9|-84|costa rican
Croatia|Хорватия|45.1|15.2|croatian,zagreb
Cuba|Куба|21.8|-79|cuban
Cyprus|Кипр|35|33|cypriot,nicosia
Czech Republic|Чехия|49.8|15.5|czech,prague
Denmark|Дания|56|10|danish,copenhagen,denmark
Dominican Republic|Доминикана|18.9|-70.2|dominican
Ecuador|Эквадор|-1.4|-78.4|ecuadorian
Egypt|Египет|26.8|30.8|egyptian
El Salvador|Сальвадор|13.7|-88.9|salvadoran
Eritrea|Эритрея|15.3|38.9|eritrean
Estonia|Эстония|58.7|25.5|estonian,tallinn
Ethiopia|Эфиопия|9|39.5|ethiopian
Finland|Финляндия|64|26|finnish,helsinki
France|Франция|46.6|2.5|french,macron,elysee
Gabon|Габон|-0.7|11.6|gabonese
Georgia (country)|Грузия|42.3|43.4|georgian,tbilisi
Germany|Германия|51.2|10.4|german,bundestag,scholz,merz
Ghana|Гана|7.9|-1|ghanaian
Greece|Греция|39|22|greek
Guatemala|Гватемала|15.5|-90.3|guatemalan
Guinea|Гвинея|10.9|-10.9|guinean
Haiti|Гаити|19|-72.7|haitian
Honduras|Гондурас|14.8|-86.6|honduran
Hungary|Венгрия|47.1|19.5|hungarian,orban
Iceland|Исландия|64.9|-18.5|icelandic
India|Индия|22.9|79.6|indian,modi
Indonesia|Индонезия|-2.5|118|indonesian
Iran|Иран|32.4|53.7|iranian,irgc
Iraq|Ирак|33|43.7|iraqi
Ireland|Ирландия|53.3|-8|irish,dublin
Israel|Израиль|31.4|35|israeli,idf,netanyahu,knesset
Italy|Италия|42.8|12.5|italian,meloni
Ivory Coast|Кот-д’Ивуар|7.6|-5.5|ivorian,cote d'ivoire
Jamaica|Ямайка|18.1|-77.3|jamaican
Japan|Япония|36.6|138.3|japanese
Jordan|Иордания|31.2|36.5|jordanian,amman
Kazakhstan|Казахстан|48|67|kazakh
Kenya|Кения|0.2|37.9|kenyan
Kosovo|Косово|42.6|20.9|kosovo,pristina
Kuwait|Кувейт|29.3|47.6|kuwaiti
Kyrgyzstan|Киргизия|41.5|74.6|kyrgyz
Laos|Лаос|19.9|102.5|laotian
Latvia|Латвия|56.9|24.9|latvian,riga
Lebanon|Ливан|33.9|35.9|lebanese,hezbollah
Libya|Ливия|27|17|libyan
Lithuania|Литва|55.3|23.9|lithuanian,vilnius
Madagascar|Мадагаскар|-19|46.7|malagasy
Malawi|Малави|-13.2|34.3|malawian
Malaysia|Малайзия|4.2|102|malaysian
Mali|Мали|17.4|-3.5|malian
Mauritania|Мавритания|20.3|-10.5|mauritanian
Mexico|Мексика|23.6|-102.5|mexican,sheinbaum
Moldova|Молдова|47.2|28.5|moldovan,chisinau
Mongolia|Монголия|46.9|103.8|mongolian
Morocco|Марокко|31.8|-7|moroccan,rabat
Mozambique|Мозамбик|-18.7|35.5|mozambican
Myanmar|Мьянма|21.2|96|burma,burmese
Namibia|Намибия|-22.4|17.1|namibian
Nepal|Непал|28.3|84.1|nepali,kathmandu
Netherlands|Нидерланды|52.2|5.6|dutch,amsterdam
New Zealand|Новая Зеландия|-41.5|172.5|new zealand,wellington,auckland
Nicaragua|Никарагуа|12.9|-85|nicaraguan
Niger|Нигер|17.4|9.4|nigerien,niamey
Nigeria|Нигерия|9.6|8.1|nigerian,abuja
North Korea|КНДР|40.2|127|north korean,kim jong
Norway|Норвегия|61|9|norwegian,oslo
Oman|Оман|21.5|56|omani,muscat
Pakistan|Пакистан|30|70|pakistani
Panama|Панама|8.5|-80|panamanian
Papua New Guinea|Папуа — Новая Гвинея|-6.4|145|papua
Paraguay|Парагвай|-23.4|-58.4|paraguayan
Peru|Перу|-9.2|-75|peruvian
Philippines|Филиппины|12.9|122.9|filipino,philippine
Poland|Польша|52|19.3|polish,tusk
Portugal|Португалия|39.6|-8|portuguese,lisbon
Qatar|Катар|25.3|51.2|qatari,doha
Romania|Румыния|45.9|25|romanian,bucharest
Russia|Россия|61.5|95|russian,putin,RUSSIA
Rwanda|Руанда|-2|29.9|rwandan,kigali
Saudi Arabia|Саудовская Аравия|24|45|saudi,riyadh
Senegal|Сенегал|14.4|-14.5|senegalese,dakar
Serbia|Сербия|44|20.8|serbian
Singapore|Сингапур|1.35|103.8|singaporean
Slovakia|Словакия|48.7|19.5|slovak
Slovenia|Словения|46.1|14.8|slovenian
Somalia|Сомали|5.2|46.2|somali
South Africa|ЮАР|-29|24|south african,pretoria
South Korea|Южная Корея|36.5|127.9|south korean
South Sudan|Южный Судан|7.3|30.3|south sudanese,juba
Spain|Испания|40.2|-3.7|spanish
Sri Lanka|Шри-Ланка|7.9|80.7|sri lankan
Sudan|Судан|15.6|30|sudanese
Sweden|Швеция|62|15|swedish,stockholm
Switzerland|Швейцария|46.8|8.2|swiss,bern
Syria|Сирия|35|38.5|syrian
Taiwan|Тайвань|23.7|121|taiwanese
Tajikistan|Таджикистан|38.9|71|tajik
Tanzania|Танзания|-6.4|34.9|tanzanian
Thailand|Таиланд|15.1|101|thai
Tunisia|Тунис|34|9.5|tunisian
Turkey|Турция|39|35.2|turkish,turkiye,erdogan
Turkmenistan|Туркмения|39|59.5|turkmen
Uganda|Уганда|1.4|32.3|ugandan,kampala
Ukraine|Украина|49|31.4|ukrainian,zelensky,zelenskyy
United Arab Emirates|ОАЭ|24.3|54.3|UAE,emirati,emirates
United Kingdom|Великобритания|54|-2.5|britain,british,UK,england,scotland,wales,starmer
United States|США|39.5|-98.5|US,U.S.,USA,american,america,trump,biden,pentagon,congress,senate
Uruguay|Уругвай|-32.8|-56|uruguayan
Uzbekistan|Узбекистан|41.4|64.6|uzbek
Venezuela|Венесуэла|7|-66|venezuelan,maduro
Vietnam|Вьетнам|16|106|vietnamese
Yemen|Йемен|15.6|47.6|yemeni
Zambia|Замбия|-13.5|27.8|zambian,lusaka
Zimbabwe|Зимбабве|-19|29.8|zimbabwean,harare
Greenland|Гренландия|72|-40|greenlandic
Hong Kong|Гонконг|22.3|114.2|hong kong
!Gaza|Газа|31.45|34.4|gaza strip,rafah,khan younis,hamas
!West Bank|Западный берег|31.95|35.25|ramallah,jenin,nablus,hebron,palestinian,palestinians
!Jerusalem|Иерусалим|31.77|35.21|jerusalem
!Tel Aviv|Тель-Авив|32.08|34.78|tel aviv
!Tehran|Тегеран|35.69|51.39|tehran
!Baghdad|Багдад|33.31|44.36|baghdad
!Damascus|Дамаск|33.51|36.28|damascus
!Aleppo|Алеппо|36.2|37.16|aleppo
!Beirut|Бейрут|33.89|35.5|beirut
!Sanaa|Сана|15.37|44.19|sanaa,houthi,houthis,hodeidah
!Red Sea|Красное море|20|38.5|red sea,bab el-mandeb
!Strait of Hormuz|Ормузский пролив|26.6|56.3|hormuz
!Gulf of Aden|Аденский залив|12.5|47|gulf of aden
!Kyiv|Киев|50.45|30.52|kyiv,kiev
!Kharkiv|Харьков|49.99|36.23|kharkiv,kharkov
!Odesa|Одесса|46.48|30.72|odesa,odessa
!Donbas|Донбасс|48.2|37.8|donbas,donetsk,luhansk,bakhmut,pokrovsk
!Zaporizhzhia|Запорожье|47.84|35.14|zaporizhzhia
!Kherson|Херсон|46.64|32.6|kherson
!Crimea|Крым|45|34|crimea,sevastopol
!Black Sea|Чёрное море|43.5|34.5|black sea
!Moscow|Москва|55.75|37.62|moscow,kremlin
!St Petersburg|Санкт-Петербург|59.93|30.34|st petersburg,st. petersburg
!Kursk|Курск|51.73|36.19|kursk,belgorod
!Minsk|Минск|53.9|27.57|minsk
!Warsaw|Варшава|52.23|21.01|warsaw
!Berlin|Берлин|52.52|13.4|berlin
!Paris|Париж|48.86|2.35|paris
!London|Лондон|51.5|-0.12|london,downing street
!Brussels|Брюссель|50.85|4.35|brussels,NATO,EU,european union,european commission
!Rome|Рим|41.9|12.5|rome,vatican
!Madrid|Мадрид|40.42|-3.7|madrid
!Budapest|Будапешт|47.5|19.04|budapest
!Athens|Афины|37.98|23.73|athens
!Istanbul|Стамбул|41.01|28.98|istanbul
!Ankara|Анкара|39.93|32.86|ankara
!Cairo|Каир|30.04|31.24|cairo
!Khartoum|Хартум|15.5|32.56|khartoum,omdurman
!Darfur|Дарфур|13|25|darfur,el fasher
!Tripoli|Триполи|32.89|13.19|tripoli
!Addis Ababa|Аддис-Абеба|9.03|38.74|addis ababa
!Tigray|Тыграй|13.5|39.5|tigray
!Nairobi|Найроби|-1.29|36.82|nairobi
!Mogadishu|Могадишо|2.05|45.32|mogadishu
!Lagos|Лагос|6.52|3.38|lagos
!Kinshasa|Киншаса|-4.44|15.27|kinshasa
!Goma|Гома|-1.68|29.22|goma,north kivu,m23
!Sahel|Сахель|14|2|sahel
!Kabul|Кабул|34.53|69.17|kabul
!Islamabad|Исламабад|33.69|73.05|islamabad
!Kashmir|Кашмир|34.1|74.8|kashmir
!New Delhi|Нью-Дели|28.61|77.21|new delhi,delhi
!Mumbai|Мумбаи|19.08|72.88|mumbai
!Dhaka|Дакка|23.81|90.41|dhaka
!Beijing|Пекин|39.9|116.4|beijing
!Shanghai|Шанхай|31.23|121.47|shanghai
!Taipei|Тайбэй|25.03|121.56|taipei
!Taiwan Strait|Тайваньский пролив|24.5|119.5|taiwan strait
!Tokyo|Токио|35.68|139.69|tokyo
!Seoul|Сеул|37.57|126.98|seoul
!Pyongyang|Пхеньян|39.04|125.76|pyongyang
!Manila|Манила|14.6|120.98|manila
!Jakarta|Джакарта|-6.2|106.85|jakarta
!Bangkok|Бангкок|13.75|100.5|bangkok
!South China Sea|Южно-Китайское море|12|114|south china sea
!Washington|Вашингтон|38.9|-77.04|washington,white house,capitol,pentagon
!New York|Нью-Йорк|40.71|-74|new york,wall street,united nations
!Los Angeles|Лос-Анджелес|34.05|-118.24|los angeles,california
!Mexico City|Мехико|19.43|-99.13|mexico city
!Ottawa|Оттава|45.42|-75.7|ottawa
!Caracas|Каракас|10.48|-66.9|caracas
!Bogota|Богота|4.71|-74.07|bogota
!Havana|Гавана|23.11|-82.37|havana
!Port-au-Prince|Порт-о-Пренс|18.54|-72.34|port-au-prince
!Brasilia|Бразилиа|-15.8|-47.9|brasilia
!Buenos Aires|Буэнос-Айрес|-34.6|-58.38|buenos aires
!Lima|Лима|-12.05|-77.04|lima
!Geneva|Женева|46.2|6.14|geneva
!The Hague|Гаага|52.08|4.3|the hague,ICC,ICJ
!Mediterranean|Средиземное море|35|18|mediterranean,lampedusa
!Baltic Sea|Балтика|58|20|baltic
!Arctic|Арктика|82|20|arctic
!Antarctica|Антарктида|-80|0|antarctic,antarctica"""


def _load_gaz():
    out = []
    for ln in GAZ_TXT.splitlines():
        key, ru, lat, lon, al = ln.split("|")
        spec, key = key.startswith("!"), key.lstrip("!")
        names = ([] if "(" in key else [key]) + [x for x in al.split(",") if x]
        pats = [re.compile(r"(?<![A-Za-z])" + re.escape(a) + r"(?![A-Za-z])", 0 if a != a.lower() else re.I) for a in names]
        out.append({"key": re.sub(r"\s*\(.*\)", "", key), "ru": ru, "lat": float(lat), "lon": float(lon), "spec": spec, "pats": pats})
    return out


GAZ = _load_gaz()


def locate(title, text):
    """Приблизительная геопривязка по названиям стран, городов и горячих точек."""
    best_c = None
    for g in GAZ:
        in_t, cnt, first = 0, 0, 10 ** 6
        for p in g["pats"]:
            cnt += len(p.findall(text))
            m = p.search(title)
            if m:
                in_t, first = 1, min(first, m.start())
        if not cnt and not in_t:
            continue
        score = (3 if in_t else 0) + (2 if g["spec"] else 0) + min(cnt, 3)
        if best_c is None or (score, -first) > best_c[0]:
            best_c = ((score, -first), g)
    if not best_c:
        return {"lat": None, "lon": None, "place": "", "place_en": ""}
    g = best_c[1]
    h = zlib.crc32(title.encode("utf-8"))
    ang, rad = (h % 360) * math.pi / 180, (0.3 if g["spec"] else 1.0) * (((h >> 9) % 100) / 100 + 0.3)
    lat = max(-85, min(85, g["lat"] + rad * math.sin(ang)))
    lon = g["lon"] + rad * math.cos(ang) / max(0.3, math.cos(math.radians(lat)))
    return {"lat": round(lat, 2), "lon": round(lon, 2), "place": g["ru"], "place_en": g["key"]}


_OG = [re.compile(r"<meta[^>]+(?:property|name)=[\"'](?:og:image|twitter:image)(?::src)?[\"'][^>]*?content=[\"']([^\"']+)", re.I),
       re.compile(r"<meta[^>]+content=[\"']([^\"']+)[\"'][^>]*?(?:property|name)=[\"'](?:og:image|twitter:image)", re.I)]


def og_image(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/html"})
        with urllib.request.urlopen(req, timeout=8) as r:
            page = r.read(300000).decode("utf-8", "ignore")
        for rx_ in _OG:
            m = rx_.search(page)
            if m:
                return norm_img(urllib.parse.urljoin(url, html.unescape(m.group(1))))
    except Exception:
        pass
    return ""


def enrich_images(events, limit=14):
    """Для сюжетов без картинки берём og:image со страницы статьи."""
    todo = [e for e in events if not e["img"] and e["u"]][:limit]
    if not todo:
        return
    with ThreadPoolExecutor(max_workers=6) as ex:
        for e, img in zip(todo, ex.map(lambda e: og_image(e["u"]), todo)):
            if img:
                e["img"], e["img_src"] = img, e["s"][0] + " (og)"
    print(f"[info] og:image найдено для {sum(1 for e in todo if e['img'])}/{len(todo)} сюжетов без картинки")


def build_timeline(items, now):
    """Реальный ряд индекса по времени публикации заголовков: окно ±1.5 ч на каждый час суток."""
    out = []
    for k in range(24, -1, -1):
        t = now - k * 3600
        w = [i["ten"] for i in items if abs(i["ts"] - t) <= 5400]
        if len(w) >= 4:
            out.append({"t": round(t * 1000), "v": round(sum(w) / len(w)), "n": len(w)})
    return out


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
            **locate(rep["title"], joined),
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

    timeline = build_timeline(allitems, now)
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
        "sources": srcv, "regions": regv, "events": events, "history": hist, "timeline": timeline,
        "feeds": [{"n": n, "items": len(by_src.get(n) or [])} for n in ORDER],
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
    need = sorted(used - set(cache))[:70]
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
    with ThreadPoolExecutor(max_workers=16) as ex:
        by_src = dict(ex.map(lambda kv: fetch_feed(*kv), SOURCES.items()))
    ok = [n for n, v in by_src.items() if v]
    print(f"[info] источников: {len(ok)}/{len(SOURCES)}, заголовков: {sum(len(v) for v in by_src.values())}")
    try:
        data = build_dataset(by_src, load_json("history.json", []))
    except RuntimeError as e:
        print(f"[warn] {e}: data.json не тронут", file=sys.stderr)
        return False
    data["failed"] = [n for n in SOURCES if n not in ok]
    enrich_images(data["events"])
    translate_events(data["events"])
    save_json("history.json", data["history"])
    save_json("data.json", data)
    print(f"[done] на карте: {sum(1 for e in data['events'] if e['lat'] is not None)}; {len(data['events'])} сюжетов, с картинками: {sum(1 for e in data['events'] if e['img'])}, {round(time.time() - t0, 1)} c")
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
