"""
Ежедневный постер новостей в Telegram-группу.

Что делает:
1. Забирает свежие записи из RSS-фидов, разбитых по категориям
   (блокировки/VPN/приватность, общие IT-новости, новинки техники/гаджеты).
2. Отбирает те, что вышли за последние N часов.
3. Ищет реальное обсуждение новости на нескольких площадках — Hacker News
   и Reddit (реальные цифры: голоса/комментарии/ссылка на обсуждение).
   Не для каждой новости и не на каждой площадке — см. условия ниже.
4. Через Anthropic API одним вызовом на весь дайджест: переводит заголовок и
   описание каждой новости на русский (сохраняя названия компаний/продуктов/имена
   как есть — не дословный перевод) и пишет короткий редакционный комментарий —
   явно как мнение ИИ-редактора, без выдумывания несуществующих фактов,
   реальных цитат пользователей или точных процентов "вероятности". Если
   ANTHROPIC_API_KEY не задан — новости остаются в оригинальном языке источника.
5. Дополнительно: ищет реальное событие "в этот день в истории" по теме IT
   через официальный API Wikipedia и добавляет его коротким пересказом —
   ИИ только переводит и оформляет уже существующий факт, не сочиняет новый.
6. Формирует один пост с разделами по категориям, отправляет в группу.
7. Записывает сегодняшние новости (с их реальными показателями по всем
   найденным источникам) в лог-файл news_log.jsonl — история для месячной статистики.
8. 1-го числа каждого месяца: находит в логе самую популярную новость месяца
   (по суммарному реальному охвату со всех источников) и публикует опрос,
   ОПИРАЯСЬ на неё, а не абстрактный вопрос в вакууме.

Настройка (через переменные окружения / GitHub Secrets):
  BOT_TOKEN         — токен вашего Telegram-бота (получить у @BotFather)
  CHAT_ID           — id группы/канала, куда постить (см. инструкцию SETUP.md)
  ANTHROPIC_API_KEY — ключ Anthropic API (console.anthropic.com), нужен для перевода
                       новостей на русский и генерации редакционных комментариев.
                       Если не задан — новости публикуются в оригинале (обычно
                       на английском), без перевода и без комментариев.

Список RSS-фидов ниже можно и нужно менять под вашу нишу.
"""

import os
import sys
import time
import html
import json
import random
import requests
import feedparser
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone

# ---------- НАСТРОЙКИ ----------

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"  # самый дешёвый/быстрый текущий Claude

# Файл-лог для месячной статистики (коммитится обратно в репозиторий Action'ом).
LOG_FILE = "news_log.jsonl"
# Сколько дней истории хранить в логе (немного больше месяца, с запасом)
LOG_RETENTION_DAYS = 35

# Сколько часов "свежести" новости считаем актуальными
LOOKBACK_HOURS = 26

# Сколько новостей максимум включать в один дайджест ИЗ КАЖДОЙ категории
MAX_ITEMS_PER_CATEGORY = 3

# Проверять реакцию на Hacker News ИМЕЕТ СМЫСЛ только для крупных англоязычных
# IT/tech-источников — там статьи реально попадают на HN и собирают обсуждение.
# Региональные источники про блокировки в РФ/СНГ (therecord, restoreprivacy,
# netblocks) почти никогда там не всплывают — для них проверка просто трата
# запроса и почти всегда пустой результат, поэтому не проверяем.
HN_CHECK_DOMAINS = {
    "techcrunch.com",
    "arstechnica.com",
    "bleepingcomputer.com",
    "theverge.com",
    "engadget.com",
}

# Reddit проверяем шире — тема блокировок/VPN/приватности там тоже реально
# обсуждается (r/privacy, r/VPN, r/technology и т.п.), в отличие от Hacker News.
# Поэтому Reddit проверяем для ВСЕХ новостей, а не только IT-доменов.
REDDIT_ENABLED = True
REDDIT_MIN_SCORE = 5  # ниже этого — считаем шумом, не показываем

# У Lobsters нет официального публичного API для поиска по URL (только
# неофициальные обходные пути), поэтому сознательно не подключаем — риск,
# что источник сломается без предупреждения, выше пользы от него.

# RSS-источники, сгруппированные по категориям.
# Ключ — заголовок раздела в посте, значение — список RSS-ссылок.
# Добавляйте/убирайте категории и фиды под свою аудиторию.
RSS_CATEGORIES = {
    "🔓 Блокировки, VPN, приватность": [
        "https://therecord.media/feed",              # кибербезопасность, блокировки
        "https://restoreprivacy.com/feed/",           # VPN, приватность
        "https://netblocks.org/feed",                 # мониторинг интернет-блокировок в мире
    ],
    "💻 IT-новости": [
        "https://www.bleepingcomputer.com/feed/",     # инфобез и IT
        "https://techcrunch.com/feed/",                # общие IT-новости
        "https://arstechnica.com/feed/",                # технологии, разборы
    ],
    "📱 Новинки техники и гаджеты": [
        "https://www.theverge.com/rss/index.xml",      # гаджеты, новые устройства
        "https://www.engadget.com/rss.xml",             # обзоры и анонсы техники
    ],
}

# Рубрика "в этот день в истории" — реальные события с Wikipedia (Wikimedia
# REST API), не выдуманные ИИ факты. Ключевые слова для отбора именно
# технологических/IT событий из общего потока (там же дни рождения королей,
# войны и т.п. — нам нужны только связанные с техникой).
HISTORY_ENABLED = True
TECH_HISTORY_KEYWORDS = [
    "computer", "software", "internet", "programming", "gnu", "linux", "unix",
    "hacker", "hacking", "technology", "microprocessor", "processor", "algorithm",
    "code", "coding", "operating system", "web", "website", "network", "email",
    "artificial intelligence", " ai ", "robot", "digital", "electronic",
    "silicon", "microchip", "chip", "database", "encryption", "cyber",
    "browser", "server", "protocol", "open source", "open-source", "app store",
    "smartphone", "telegram", "google", "apple inc", "microsoft", "ibm",
]

HEADER_TEXT = "📰 <b>Новости дня</b>\n"
FOOTER_TEXT = "\n\n🔓 Обходите блокировки через нашего бота: @wuwei_flow_bot"

# Варианты ответа для ежемесячного опроса — про них у нас нет реальных данных
# (это будущие предпочтения аудитории), поэтому они остаются вопросом, а не
# утверждением. А вот КОНТЕКСТ перед опросом строится на реальных цифрах —
# см. build_monthly_recap().
MONTHLY_POLL_OPTIONS = [
    "Да, больше такого",
    "Нет, не особо",
    "Хочу другую тему",
]

# ---------- ЛОГИКА ----------


def fetch_entries_for_feeds(feed_urls, cutoff):
    entries = []
    for feed_url in feed_urls:
        try:
            feed = feedparser.parse(feed_url)
        except Exception as e:
            print(f"[WARN] Не удалось прочитать {feed_url}: {e}")
            continue

        for entry in feed.entries:
            published = entry.get("published_parsed") or entry.get("updated_parsed")
            if not published:
                continue
            published_dt = datetime.fromtimestamp(time.mktime(published), tz=timezone.utc)
            if published_dt < cutoff:
                continue

            title = html.unescape(entry.get("title", "").strip())
            link = entry.get("link", "").strip()
            summary_raw = entry.get("summary", "") or entry.get("description", "")
            summary = html.unescape(_strip_html(summary_raw)).strip()
            summary = (summary[:180] + "…") if len(summary) > 180 else summary

            if title and link:
                entries.append({
                    "title": title,
                    "link": link,
                    "summary": summary,
                    "published": published_dt,
                    "source": feed.feed.get("title", feed_url),
                })

    entries.sort(key=lambda e: e["published"], reverse=True)
    return entries[:MAX_ITEMS_PER_CATEGORY]


def fetch_all_categories():
    """Возвращает dict {категория: [новости]}, пропуская пустые категории.
    К каждой новости добавляет поле "reactions" — dict с реальной реакцией
    по каждому найденному источнику (Hacker News, Reddit). Источник опускается,
    если проверка для него не имеет смысла для этого домена или обсуждение
    не нашлось."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    result = {}
    for category, feed_urls in RSS_CATEGORIES.items():
        entries = fetch_entries_for_feeds(feed_urls, cutoff)
        for e in entries:
            e["reactions"] = fetch_reactions(e["link"])
        if entries:
            result[category] = entries
    return result


def fetch_reactions(url):
    """Собирает реальную реакцию на новость с нескольких площадок.
    Возвращает dict {source_key: {label, points, comments, discussion_url}},
    источники без реального результата в словарь не попадают."""
    reactions = {}

    domain = urlparse(url).netloc.lower().removeprefix("www.")
    if domain in HN_CHECK_DOMAINS:
        hn = fetch_hn_reaction(url)
        if hn:
            reactions["hn"] = hn

    if REDDIT_ENABLED:
        reddit = fetch_reddit_reaction(url)
        if reddit:
            reactions["reddit"] = reddit

    return reactions


def _strip_html(text):
    import re
    return re.sub("<[^<]+?>", "", text)


def fetch_hn_reaction(url):
    """Ищет реальное обсуждение статьи на Hacker News через Algolia API.
    Возвращает {label, points, comments, discussion_url} или None.
    Это НАСТОЯЩИЕ данные, не сгенерированные."""
    try:
        resp = requests.get(
            "https://hn.algolia.com/api/v1/search",
            params={"query": url, "restrictSearchableAttributes": "url", "tags": "story"},
            timeout=10,
        )
        resp.raise_for_status()
        hits = resp.json().get("hits", [])
        if not hits:
            return None
        top = hits[0]
        if top.get("num_comments", 0) < 3:  # слишком мало обсуждения — не показываем
            return None
        return {
            "label": "Hacker News",
            "points": top.get("points", 0),
            "comments": top.get("num_comments", 0),
            "discussion_url": f"https://news.ycombinator.com/item?id={top['objectID']}",
        }
    except Exception as e:
        print(f"[WARN] Не удалось получить реакцию HN для {url}: {e}")
        return None


def fetch_reddit_reaction(url):
    """Ищет реальные посты со ссылкой на статью через публичный поиск Reddit
    (reddit.com/search.json, без авторизации, но с обязательным User-Agent).
    Возвращает {label, points, comments, discussion_url} по самому заметному
    посту, или None, если ничего подходящего не нашлось."""
    try:
        resp = requests.get(
            "https://www.reddit.com/search.json",
            params={"q": f'url:"{url}"', "sort": "top", "t": "year", "limit": 5},
            headers={"User-Agent": "tg-news-bot/1.0 (daily digest script)"},
            timeout=10,
        )
        resp.raise_for_status()
        children = resp.json().get("data", {}).get("children", [])
        if not children:
            return None
        top = max((c["data"] for c in children), key=lambda d: d.get("score", 0))
        if top.get("score", 0) < REDDIT_MIN_SCORE:
            return None
        return {
            "label": f"Reddit (r/{top.get('subreddit', '?')})",
            "points": top.get("score", 0),
            "comments": top.get("num_comments", 0),
            "discussion_url": f"https://reddit.com{top.get('permalink', '')}",
        }
    except Exception as e:
        print(f"[WARN] Не удалось получить реакцию Reddit для {url}: {e}")
        return None


def fetch_tech_history_event():
    """Берёт реальные события 'в этот день' с официального Wikimedia REST API
    и отбирает те, что связаны с технологиями/IT, по ключевым словам.
    Возвращает {year, text, wiki_url} по случайно выбранному подходящему
    событию, или None, если сегодня подходящих событий не нашлось — тогда
    рубрика просто не появляется в посте, ничего не выдумывается."""
    if not HISTORY_ENABLED:
        return None

    today = datetime.now(timezone.utc)
    month, day = f"{today.month:02d}", f"{today.day:02d}"

    try:
        resp = requests.get(
            f"https://en.wikipedia.org/api/rest_v1/feed/onthisday/events/{month}/{day}",
            headers={"User-Agent": "tg-news-bot/1.0 (Telegram digest; contact: set-your-email@example.com)"},
            timeout=15,
        )
        resp.raise_for_status()
        events = resp.json().get("events", [])
    except Exception as e:
        print(f"[WARN] Не удалось получить события 'в этот день' с Wikipedia: {e}")
        return None

    tech_events = [
        ev for ev in events
        if any(kw in f" {ev.get('text', '').lower()} " for kw in TECH_HISTORY_KEYWORDS)
    ]
    if not tech_events:
        return None

    event = random.choice(tech_events)
    pages = event.get("pages") or []
    wiki_url = None
    if pages:
        wiki_url = pages[0].get("content_urls", {}).get("desktop", {}).get("page")

    return {
        "year": event.get("year"),
        "text": event.get("text", ""),
        "wiki_url": wiki_url or "https://en.wikipedia.org/wiki/Portal:Technology",
    }


def generate_history_blurb(event):
    """Отдельным небольшим вызовом Anthropic API переводит и слегка оживляет
    исходный факт с Wikipedia для Telegram-поста. НЕ добавляет новых фактов,
    дат или деталей сверх того, что реально пришло с Wikipedia — задача модели
    здесь только перевод и стиль, не сочинительство. Возвращает None, если
    ANTHROPIC_API_KEY не задан или запрос не удался (тогда рубрика просто
    не публикуется — не постим необработанный английский текст посреди
    русскоязычного канала)."""
    if not ANTHROPIC_API_KEY or not event:
        return None

    system_prompt = (
        "Тебе дан реальный исторический факт с Wikipedia (год и описание события на английском). "
        "Перепиши его по-русски живо и интересно для Telegram-канала про IT/VPN, 2-3 предложения. "
        "Переведи точно — НЕ добавляй никаких дат, цифр, имён или деталей, которых нет в исходном "
        "тексте, ничего не выдумывай. Названия компаний/продуктов оставь как принято в русской "
        "IT-прессе. Ответь только текстом заметки, без кавычек, без вступления вроде 'Вот заметка:'."
    )
    user_content = f"Год: {event['year']}. Событие: {event['text']}"

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": ANTHROPIC_MODEL,
                "max_tokens": 300,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_content}],
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        text = "".join(block.get("text", "") for block in data.get("content", []))
        return text.strip() or None
    except Exception as e:
        print(f"[WARN] Не удалось получить пересказ исторического факта: {e}")
        return None


def build_history_block(event, blurb):
    """Собирает готовый блок рубрики 'в этот день'. Возвращает None, если
    нет ни события, ни пересказа (значит, блок не публикуется вовсе)."""
    if not event or not blurb:
        return None
    return (
        f"\n📅 <b>Это было в истории — {event['year']} год</b>\n"
        f"{html.escape(blurb)}\n"
        f"<a href=\"{event['wiki_url']}\">Подробнее на Wikipedia →</a>\n"
    )


def translate_and_annotate(categorized_entries):
    """Одним вызовом Anthropic API для каждой новости:
    1) переводит заголовок и описание на русский (сохраняя имена собственные,
       названия компаний/продуктов в общепринятом виде — не дословный перевод);
    2) пишет короткий редакционный комментарий на русском.
    Возвращает dict {(category, index): {"title", "summary", "note"}} либо {}
    при ошибке или если ANTHROPIC_API_KEY не задан (тогда новости остаются
    в оригинале — см. build_message)."""
    if not ANTHROPIC_API_KEY:
        return {}

    flat_items = []
    keys = []
    for category, entries in categorized_entries.items():
        for i, e in enumerate(entries):
            flat_items.append({"title": e["title"], "summary": e["summary"]})
            keys.append((category, i))

    if not flat_items:
        return {}

    system_prompt = (
        "Ты — редактор новостного Telegram-дайджеста про VPN, приватность и IT. "
        "Для каждой новости на входе (title, summary на английском или другом языке) сделай два дела:\n\n"
        "1) ПЕРЕВОД: переведи title и summary на естественный русский язык, как для "
        "нормального русскоязычного IT-издания. НЕ переводи дословно и НЕ переводи: "
        "названия компаний и продуктов (Apple, Google, iPhone, ChatGPT), имена людей, "
        "названия языков программирования и технологий, общепринятые англицизмы, которые "
        "и в русской IT-прессе пишут латиницей. Всё остальное — обычные слова, термины, "
        "описания событий — переведи полностью, без английских вставок.\n\n"
        "2) КОММЕНТАРИЙ: короткий редакционный комментарий на русском (2-3 предложения) — "
        "почему это важно и что это может означать для читателя. Можешь аккуратно "
        "порассуждать о вероятном развитии ситуации, но ТОЛЬКО как рассуждение "
        "('вероятно', 'возможно', 'если тенденция продолжится') — НЕ придумывай точные цифры, "
        "проценты, статистику или несуществующие цитаты пользователей. Если фактов недостаточно "
        "для оценки — просто опиши значимость новости без прогноза.\n\n"
        "Ответь СТРОГО в формате JSON-массива объектов, без markdown-разметки, без пояснений:\n"
        '[{"title": "переведённый заголовок", "summary": "переведённое описание", '
        '"note": "комментарий"}, ...]\n'
        f"Число новостей: {len(flat_items)}, порядок сохрани как во входных данных."
    )

    user_content = json.dumps(flat_items, ensure_ascii=False)

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": ANTHROPIC_MODEL,
                "max_tokens": 2048,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_content}],
            },
            timeout=90,
        )
        resp.raise_for_status()
        data = resp.json()
        text = "".join(block.get("text", "") for block in data.get("content", []))
        text = text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        results = json.loads(text)
        if len(results) != len(keys):
            print("[WARN] Число AI-результатов не совпало с числом новостей, пропускаю.")
            return {}
        return dict(zip(keys, results))
    except Exception as e:
        print(f"[WARN] Не удалось получить перевод и AI-комментарии: {e}")
        return {}


def build_message(categorized_entries, ai_results, history_block=None):
    if not categorized_entries:
        return None

    lines = [HEADER_TEXT]
    for category, entries in categorized_entries.items():
        lines.append(f"\n<b>{html.escape(category)}</b>")
        for i, e in enumerate(entries):
            ai = ai_results.get((category, i), {})
            # Если перевода нет (нет ключа API или сбой запроса) — используем
            # оригинал, чтобы пост в любом случае вышел, пусть и не на русском.
            title = ai.get("title") or e["title"]
            summary = ai.get("summary") or e["summary"]

            block = (
                f"▪️ <b>{html.escape(title)}</b>\n"
                f"{html.escape(summary)}\n"
                f"<a href=\"{e['link']}\">Читать →</a> · <i>{html.escape(e['source'])}</i>"
            )

            for r in e.get("reactions", {}).values():
                block += (
                    f"\n🗣 <a href=\"{r['discussion_url']}\">{html.escape(r['label'])}</a>: "
                    f"{r['points']} голосов, {r['comments']} комментариев"
                )

            note = ai.get("note")
            if note:
                block += f"\n💬 <i>Мнение редакции: {html.escape(note)}</i>"

            lines.append(block + "\n")

    if history_block:
        lines.append(history_block)

    lines.append(FOOTER_TEXT)
    return "\n".join(lines)


def append_to_log(categorized_entries, ai_results):
    """Дописывает сегодняшние новости (с реальными показателями по всем найденным
    источникам) в LOG_FILE построчно в формате JSON. Заголовок сохраняется в
    переведённом виде (если перевод получился) — чтобы месячный итог тоже был
    на русском. Не хранит AI-комментарии — только факты."""
    today = datetime.now(timezone.utc).date().isoformat()
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            for category, entries in categorized_entries.items():
                for i, e in enumerate(entries):
                    title = ai_results.get((category, i), {}).get("title") or e["title"]
                    row = {
                        "date": today,
                        "category": category,
                        "title": title,
                        "link": e["link"],
                        "source": e["source"],
                        "reactions": e.get("reactions", {}),
                    }
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[WARN] Не удалось записать лог: {e}")


def prune_log():
    """Оставляет в LOG_FILE только записи не старше LOG_RETENTION_DAYS, чтобы
    файл не рос бесконечно."""
    if not os.path.exists(LOG_FILE):
        return
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=LOG_RETENTION_DAYS)).isoformat()
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            rows = [json.loads(line) for line in f if line.strip()]
        kept = [r for r in rows if r.get("date", "") >= cutoff]
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            for r in kept:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[WARN] Не удалось очистить старый лог: {e}")


def find_top_story_last_30_days():
    """Читает LOG_FILE и находит новость с наибольшим суммарным реальным охватом
    (сумма голосов со всех источников, где нашлось обсуждение) за последние ~30 дней.
    Возвращает запись из лога или None, если подходящих записей не нашлось —
    тогда опрос строится без привязки к конкретной новости, честно про это сообщая."""
    if not os.path.exists(LOG_FILE):
        return None
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=30)).isoformat()
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            rows = [json.loads(line) for line in f if line.strip()]
    except Exception as e:
        print(f"[WARN] Не удалось прочитать лог: {e}")
        return None

    candidates = [r for r in rows if r.get("date", "") >= cutoff and r.get("reactions")]
    if not candidates:
        return None

    def total_engagement(row):
        return sum(r.get("points", 0) for r in row["reactions"].values())

    return max(candidates, key=total_engagement)


def build_monthly_recap(top_story):
    """Строит текст итога месяца. Если реальных данных о популярности нет —
    честно говорит об этом, а не выдумывает победителя."""
    if top_story is None:
        return (
            "📊 <b>Итоги месяца</b>\n\n"
            "За последний месяц у нас пока недостаточно данных о реальной "
            "популярности новостей (не было заметных обсуждений ни на одной из "
            "проверяемых площадок), поэтому в этот раз без конкретного лидера — "
            "просто спросим напрямую 👇"
        )

    reaction_lines = "\n".join(
        f"🗣 <a href=\"{r['discussion_url']}\">{html.escape(r['label'])}</a>: "
        f"{r['points']} голосов, {r['comments']} комментариев"
        for r in top_story["reactions"].values()
    )

    return (
        "📊 <b>Итоги месяца: самая обсуждаемая новость</b>\n\n"
        f"<b>{html.escape(top_story['title'])}</b>\n"
        f"{reaction_lines}\n"
        f"<a href=\"{top_story['link']}\">Читать оригинал →</a>\n\n"
        "<i>Рейтинг — по суммарному числу голосов со всех проверяемых площадок "
        "(Hacker News, Reddit). Площадки считают популярность по-разному, так что "
        "это ориентировочная, а не строго научная метрика.</i>"
    )
def send_to_telegram(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    resp = requests.post(url, data=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def send_monthly_recap_and_poll():
    """Раз в месяц (1-го числа): сначала публикует реальную статистику —
    самую обсуждаемую новость месяца по данным Hacker News — затем задаёт
    опрос уже в привязке к этому контексту, а не абстрактно."""
    top_story = find_top_story_last_30_days()
    recap_text = build_monthly_recap(top_story)

    recap_result = send_to_telegram(recap_text)

    if top_story:
        poll_question = f"Хотите больше новостей в духе «{top_story['title'][:150]}»?"
    else:
        poll_question = "Каких новостей вам хочется больше в этом канале?"

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPoll"
    payload = {
        "chat_id": CHAT_ID,
        "question": poll_question,
        "options": json.dumps(MONTHLY_POLL_OPTIONS, ensure_ascii=False),
        "is_anonymous": True,
        "allows_multiple_answers": False,
    }
    resp = requests.post(url, data=payload, timeout=30)
    resp.raise_for_status()
    poll_result = resp.json()

    return recap_result, poll_result


def main():
    if not BOT_TOKEN or not CHAT_ID:
        print("[ERROR] Не заданы переменные окружения BOT_TOKEN и/или CHAT_ID.")
        sys.exit(1)

    categorized_entries = fetch_all_categories()
    if not categorized_entries:
        print("[INFO] Свежих новостей за последние", LOOKBACK_HOURS, "часов не найдено. Пост не отправлен.")
        return

    ai_results = translate_and_annotate(categorized_entries)

    history_event = fetch_tech_history_event()
    history_blurb = generate_history_blurb(history_event)
    history_block = build_history_block(history_event, history_blurb)

    message = build_message(categorized_entries, ai_results, history_block)
    result = send_to_telegram(message)
    print("[OK] Пост отправлен:", result.get("ok"))

    append_to_log(categorized_entries, ai_results)
    prune_log()

    # 1-го числа каждого месяца — итог месяца по реальным данным + опрос на этой основе
    if datetime.now(timezone.utc).day == 1:
        recap_result, poll_result = send_monthly_recap_and_poll()
        print("[OK] Итог месяца отправлен:", recap_result.get("ok"))
        print("[OK] Ежемесячный опрос отправлен:", poll_result.get("ok"))


if __name__ == "__main__":
    main()
