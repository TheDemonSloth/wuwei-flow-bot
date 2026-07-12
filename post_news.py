"""
Ежедневный постер новостей в Telegram-группу.

Что делает:
1. Забирает свежие записи из RSS-фидов, разбитых по категориям (блокировки/VPN/
   приватность, IT-новости, гаджеты, российские IT-новости, крипта).
2. Отбирает те, что вышли за последние N часов и ещё не публиковались раньше
   (сверяется с news_log.jsonl). Итоговое число новостей за день ограничено
   TOTAL_MAX_ITEMS_PER_DAY — отбор идёт по кругу (round-robin) между
   категориями, чтобы темы были представлены равномерно.
3. Для каждой новости пытается найти картинку — сперва в самой RSS-записи,
   при отсутствии — через og:image со страницы статьи.
4. Ищет реальное обсуждение новости на нескольких площадках — Hacker News
   и Reddit (реальные цифры: голоса/комментарии/ссылка на обсуждение).
   Не для каждой новости и не на каждой площадке — см. условия ниже.
5. Через Anthropic API одним вызовом на весь дайджест: переводит заголовок и
   описание каждой новости на русский (сохраняя названия компаний/продуктов/имена
   как есть — не дословный перевод) и пишет короткий редакционный комментарий —
   явно как мнение ИИ-редактора, без выдумывания несуществующих фактов,
   реальных цитат пользователей или точных процентов "вероятности". Если
   ANTHROPIC_API_KEY не задан — новости остаются в оригинальном языке источника.
6. Дополнительно: ищет реальное событие "в этот день в истории" по теме IT
   через официальный API Wikipedia и добавляет его коротким пересказом —
   ИИ только переводит и оформляет уже существующий факт, не сочиняет новый.
7. Публикует новости РАСТЯНУТО в течение дня — не все сразу, а с случайным
   интервалом от MIN_GAP_HOURS до MAX_GAP_HOURS часов между постами. Каждая
   новость выходит ОТДЕЛЬНЫМ постом с картинкой (если нашлась; если нет —
   постом без картинки, обычным текстом). После того как разошлись все
   новости за день — отдельно рубрика истории, затем сообщение со ссылкой
   на бота. Расписание и очередь публикаций хранятся в post_queue.json
   (коммитится обратно в репозиторий) — сам скрипт "не спит" между постами,
   а просто запускается заново каждый час (см. daily-post.yml) и каждый раз
   смотрит в очередь, не пора ли публиковать следующую новость.
8. Записывает сегодняшние новости (с их реальными показателями по всем
   найденным источникам) в лог-файл news_log.jsonl — история для месячной статистики.
9. 1-го числа каждого месяца, после того как весь план публикаций на день
   выполнен: находит в логе самую популярную новость месяца (по суммарному
   реальному охвату со всех источников) и публикует опрос, ОПИРАЯСЬ на неё,
   а не абстрактный вопрос в вакууме.

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
import re
import requests
import feedparser
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone

# ---------- НАСТРОЙКИ ----------

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
# При ручном запуске workflow (workflow_dispatch) можно поставить галочку
# "force_now" — тогда новая партия новостей соберётся сразу, не дожидаясь
# DAILY_FETCH_HOUR_UTC. По расписанию (cron) эта переменная не задаётся,
# так что автоматические запуски всегда работают по обычным правилам.
FORCE_NOW = os.environ.get("FORCE_NOW", "").lower() == "true"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"  # самый дешёвый/быстрый текущий Claude

# Файл-лог для месячной статистики (коммитится обратно в репозиторий Action'ом).
LOG_FILE = "news_log.jsonl"
# Сколько дней истории хранить в логе (немного больше месяца, с запасом)
LOG_RETENTION_DAYS = 35

# Файл-очередь публикаций на сегодня (тоже коммитится обратно в репозиторий).
# Нужен, чтобы растягивать посты по времени между отдельными запусками
# workflow — сам скрипт не "спит", он просто каждый раз запускается заново
# и смотрит, не пора ли опубликовать следующую новость из очереди.
QUEUE_FILE = "post_queue.json"

# В какой час (UTC) собирать новую партию новостей на день. 6 UTC = 9:00 МСК.
# Как только наступает новый день (по UTC) и текущий час совпадает с этим —
# скрипт формирует очередь на весь день; остальные часы просто ждут.
DAILY_FETCH_HOUR_UTC = 6

# Разброс между публикациями отдельных новостей — от MIN_GAP_HOURS до
# MAX_GAP_HOURS часов, выбирается случайно для каждого интервала. Из-за этого
# точное время публикации каждой следующей новости заранее непредсказуемо —
# это осознанный выбор, не баг.
MIN_GAP_HOURS = 3
MAX_GAP_HOURS = 6

# Сколько часов "свежести" новости считаем актуальными
LOOKBACK_HOURS = 26

# Сколько новостей максимум брать "в кандидаты" ИЗ КАЖДОЙ категории — это ещё
# не финальное число в посте, а пул, из которого потом выбирается TOTAL_MAX_ITEMS_PER_DAY
# новостей суммарно по всем категориям (см. ниже).
MAX_ITEMS_PER_CATEGORY = 2

# Сколько новостей публиковать в дайджесте СУММАРНО по всем категориям вместе.
# Отбор идёт по кругу (round-robin) — по одной новости из каждой категории по
# очереди, пока не наберётся этот лимит — так в посте стараются быть представлены
# разные темы, а не только та категория, где сегодня вышло больше всего статей.
TOTAL_MAX_ITEMS_PER_DAY = 4

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
    "🇷🇺 Российские IT-новости": [
        "https://habr.com/ru/rss/all/",                 # крупнейший русскоязычный IT-ресурс.
        # Уже на русском — переводить не нужно, работает даже без ANTHROPIC_API_KEY.
    ],
    "💰 Крипта": [
        "https://cointelegraph.com/rss",                # одно из крупнейших крипто-изданий
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

HEADER_TEXT = "📰 <b>Новости дня</b>"
FOOTER_TEXT = "🔓 Обходите блокировки через нашего бота: @NetBotCube_Bot"

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


def load_posted_links():
    """Читает LOG_FILE и возвращает множество ссылок, которые уже публиковались
    (за весь срок хранения лога, LOG_RETENTION_DAYS). Используется, чтобы не
    постить одну и ту же новость повторно — например, при ручном перезапуске
    workflow или если за сутки вышло мало новых статей."""
    if not os.path.exists(LOG_FILE):
        return set()
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            return {json.loads(line)["link"] for line in f if line.strip()}
    except Exception as e:
        print(f"[WARN] Не удалось прочитать лог для дедупликации: {e}")
        return set()


def get_rss_image(raw_entry):
    """Пытается найти картинку прямо в данных RSS/Atom-записи (media:thumbnail,
    media:content, enclosure) — без дополнительных сетевых запросов. Возвращает
    URL картинки или None."""
    try:
        thumbs = getattr(raw_entry, "media_thumbnail", None)
        if thumbs:
            return thumbs[0].get("url")
        media = getattr(raw_entry, "media_content", None)
        if media:
            for m in media:
                if m.get("url"):
                    return m.get("url")
        for link in raw_entry.get("links", []):
            if link.get("rel") == "enclosure" and link.get("type", "").startswith("image"):
                return link.get("href")
    except Exception:
        pass
    return None


def fetch_og_image(article_url):
    """Фолбэк, если в самом RSS картинки не было: скачивает HTML страницы
    новости и ищет мета-тег og:image. Вызывается только для уже ОТОБРАННЫХ
    в дайджест новостей (не для всех кандидатов) — чтобы не тратить лишние
    запросы. Возвращает URL картинки или None, если не нашлось/не получилось."""
    try:
        resp = requests.get(
            article_url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; tg-news-bot/1.0)"},
            timeout=10,
        )
        match = re.search(
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
            resp.text,
            re.IGNORECASE,
        )
        if match:
            return match.group(1)
    except Exception as e:
        print(f"[WARN] Не удалось получить og:image для {article_url}: {e}")
    return None


# Цвета заглушки под каждую категорию (фон, текст — hex без "#") и короткая
# ASCII/кириллическая подпись без эмодзи (эмодзи в URL генератора не нужны).
PLACEHOLDER_STYLE = {
    "🔓 Блокировки, VPN, приватность": ("7f1d1d", "ffffff", "VPN"),
    "💻 IT-новости": ("1e3a8a", "ffffff", "IT News"),
    "📱 Новинки техники и гаджеты": ("581c87", "ffffff", "Gadgets"),
    "🇷🇺 Российские IT-новости": ("14532d", "ffffff", "Хабр"),
    "💰 Крипта": ("78350f", "ffffff", "Crypto"),
}
PLACEHOLDER_DEFAULT_STYLE = ("1f2937", "ffffff", "无为 WuWei")


def generate_placeholder_image(category):
    """Последний, ГАРАНТИРОВАННЫЙ уровень: если ни в RSS, ни на странице
    статьи не нашлось картинки, генерирует простую цветную заглушку с
    подписью категории через бесплатный сервис placehold.co (без ключей,
    работает по прямой ссылке). Так у поста ВСЕГДА будет картинка."""
    from urllib.parse import quote
    bg, fg, label = PLACEHOLDER_STYLE.get(category, PLACEHOLDER_DEFAULT_STYLE)
    return f"https://placehold.co/1200x630/{bg}/{fg}?text={quote(label)}&font=roboto"


def fetch_entries_for_feeds(feed_urls, cutoff, posted_links):
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

            if link in posted_links:  # уже публиковали — пропускаем
                continue

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
                    "image": get_rss_image(entry),  # может быть None — дозаполним позже
                })

    entries.sort(key=lambda e: e["published"], reverse=True)
    return entries[:MAX_ITEMS_PER_CATEGORY]


def fetch_all_categories():
    """Возвращает dict {категория: [новости]}, пропуская пустые категории.
    Новости, которые уже публиковались раньше (есть в news_log.jsonl), сюда
    не попадают — см. load_posted_links(). Итоговое число новостей в дайджесте
    ограничено TOTAL_MAX_ITEMS_PER_DAY суммарно по всем категориям — отбор идёт
    по кругу (round-robin), чтобы разные темы были представлены равномерно,
    а не только та категория, где сегодня вышло больше всего статей.
    К каждой оставшейся новости добавляет поле "reactions" — dict с реальной
    реакцией по каждому найденному источнику (Hacker News, Reddit). Источник
    опускается, если проверка для него не имеет смысла для этого домена или
    обсуждение не нашлось."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    posted_links = load_posted_links()

    candidates = {}
    for category, feed_urls in RSS_CATEGORIES.items():
        entries = fetch_entries_for_feeds(feed_urls, cutoff, posted_links)
        if entries:
            candidates[category] = entries

    # Round-robin: по одной новости из каждой категории по очереди,
    # пока не наберём общий лимит или не кончатся кандидаты во всех категориях.
    result = {}
    queues = {cat: list(entries) for cat, entries in candidates.items()}
    total_selected = 0
    while total_selected < TOTAL_MAX_ITEMS_PER_DAY and any(queues.values()):
        for category in list(RSS_CATEGORIES.keys()):
            if total_selected >= TOTAL_MAX_ITEMS_PER_DAY:
                break
            queue = queues.get(category)
            if not queue:
                continue
            entry = queue.pop(0)
            entry["reactions"] = fetch_reactions(entry["link"])
            if not entry.get("image"):  # в RSS картинки не было — пробуем со страницы
                entry["image"] = fetch_og_image(entry["link"])
            if not entry.get("image"):  # и там не нашлось — гарантированная заглушка
                entry["image"] = generate_placeholder_image(category)
            result.setdefault(category, []).append(entry)
            total_selected += 1

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
    """Собирает готовый блок рубрики 'в этот день' как отдельное сообщение.
    Возвращает None, если нет ни события, ни пересказа (значит, сообщение
    не публикуется вовсе)."""
    if not event or not blurb:
        return None
    return (
        f"📅 <b>Это было в истории — {event['year']} год</b>\n"
        f"{html.escape(blurb)}\n"
        f"<a href=\"{event['wiki_url']}\">Подробнее на Wikipedia →</a>"
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


def build_item_caption(category, entry, ai):
    """Собирает подпись для ОДНОГО поста с картинкой. Telegram ограничивает
    подписи к фото 1024 символами (жёстче, чем 4096 у обычного текста) —
    поэтому при нехватке места функция прогрессивно жертвует менее важными
    частями: сначала мнением редакции, потом блоками реакций, и только в
    крайнем случае обрезает само описание."""
    CAPTION_LIMIT = 1024

    title = ai.get("title") or entry["title"]
    summary = ai.get("summary") or entry["summary"]
    note = ai.get("note")

    category_line = f"<b>{html.escape(category)}</b>\n"
    title_line = f"<b>{html.escape(title)}</b>\n"
    link_line = f"<a href=\"{entry['link']}\">Читать →</a> · <i>{html.escape(entry['source'])}</i>"
    reactions_lines = "".join(
        f"\n🗣 <a href=\"{r['discussion_url']}\">{html.escape(r['label'])}</a>: "
        f"{r['points']} голосов, {r['comments']} комментариев"
        for r in entry.get("reactions", {}).values()
    )
    note_line = f"\n💬 <i>Мнение редакции: {html.escape(note)}</i>" if note else ""

    def assemble(summary_text, include_reactions, include_note):
        return (
            category_line + title_line + html.escape(summary_text) + "\n" + link_line
            + (reactions_lines if include_reactions else "")
            + (note_line if include_note else "")
        )

    caption = assemble(summary, True, True)
    if len(caption) <= CAPTION_LIMIT:
        return caption

    caption = assemble(summary, True, False)  # жертвуем мнением редакции
    if len(caption) <= CAPTION_LIMIT:
        return caption

    caption = assemble(summary, False, False)  # жертвуем блоками реакций
    if len(caption) <= CAPTION_LIMIT:
        return caption

    # В крайнем случае обрезаем само описание, оставляя место под остальное.
    overhead = len(category_line + title_line + "\n" + link_line)
    max_summary_len = max(0, CAPTION_LIMIT - overhead - 1)
    trimmed = summary[:max_summary_len].rstrip() + "…"
    return category_line + title_line + html.escape(trimmed) + "\n" + link_line


def append_single_to_log(item):
    """Дописывает ОДНУ уже опубликованную новость в LOG_FILE. Заголовок
    сохраняется в переведённом виде (если перевод получился) — чтобы месячный
    итог тоже был на русском. Не хранит AI-комментарии — только факты."""
    row = {
        "date": datetime.now(timezone.utc).date().isoformat(),
        "category": item["category"],
        "title": item.get("log_title") or item["title"],
        "link": item["link"],
        "source": item["source"],
        "reactions": item.get("reactions", {}),
    }
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
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


def send_photo_to_telegram(photo_url, caption):
    """Публикует новость как фото с подписью (sendPhoto). Если Telegram не
    смог получить картинку по ссылке (битый URL, сайт блокирует хотлинки и
    т.п.) — выбрасывает исключение, и main() сам решает откатиться на
    обычное текстовое сообщение (см. send_to_telegram)."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    payload = {
        "chat_id": CHAT_ID,
        "photo": photo_url,
        "caption": caption,
        "parse_mode": "HTML",
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


def load_queue():
    """Читает QUEUE_FILE (очередь публикаций на сегодня). Возвращает None,
    если файла нет или он повреждён — тогда main() решит, что пора собирать
    новую очередь."""
    if not os.path.exists(QUEUE_FILE):
        return None
    try:
        with open(QUEUE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] Не удалось прочитать {QUEUE_FILE}: {e}")
        return None


def save_queue(state):
    try:
        with open(QUEUE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARN] Не удалось сохранить {QUEUE_FILE}: {e}")


def build_new_queue():
    """Собирает новости на сегодня и планирует время публикации каждой —
    первая уходит сразу, следующие — через случайный интервал от MIN_GAP_HOURS
    до MAX_GAP_HOURS часов одна от другой. Перевод и AI-комментарии считаются
    здесь же, один раз на весь день (не при каждой публикации) — экономит
    вызовы API. Возвращает готовое состояние очереди или None, если сегодня
    свежих новостей не нашлось вовсе."""
    categorized_entries = fetch_all_categories()
    if not categorized_entries:
        return None

    ai_results = translate_and_annotate(categorized_entries)

    items = []
    post_at = datetime.now(timezone.utc)
    for category, entries in categorized_entries.items():
        for i, entry in enumerate(entries):
            ai = ai_results.get((category, i), {})
            items.append({
                "category": category,
                "link": entry["link"],
                "title": entry["title"],
                "log_title": ai.get("title") or entry["title"],
                "source": entry["source"],
                "image": entry.get("image"),
                "caption": build_item_caption(category, entry, ai),
                "reactions": entry.get("reactions", {}),
                "post_at": post_at.isoformat(),
                "posted": False,
            })
            post_at = post_at + timedelta(hours=random.uniform(MIN_GAP_HOURS, MAX_GAP_HOURS))

    return {
        "date": datetime.now(timezone.utc).date().isoformat(),
        "items": items,
        "history_posted": False,
        "footer_posted": False,
        "monthly_sent": False,
    }


def main():
    if not BOT_TOKEN or not CHAT_ID:
        print("[ERROR] Не заданы переменные окружения BOT_TOKEN и/или CHAT_ID.")
        sys.exit(1)

    now = datetime.now(timezone.utc)
    today = now.date().isoformat()
    state = load_queue()

    # Новый день (или очереди ещё не было) — собираем новую партию новостей,
    # но только в заданный час, чтобы не постить в случайное время суток.
    # Исключение — принудительный ручной запуск (FORCE_NOW), для тестирования.
    if state is None or state.get("date") != today:
        if now.hour != DAILY_FETCH_HOUR_UTC and not FORCE_NOW:
            print(f"[INFO] Ждём {DAILY_FETCH_HOUR_UTC}:00 UTC для сбора новой партии новостей "
                  f"(сейчас {now.hour}:00 UTC). Для теста запустите workflow вручную "
                  f"с галочкой force_now.")
            return
        new_state = build_new_queue()
        if new_state is None:
            print("[INFO] Свежих новостей за последние", LOOKBACK_HOURS, "часов не найдено.")
            return
        state = new_state
        send_to_telegram(HEADER_TEXT)
        save_queue(state)

    # Публикуем максимум ОДНУ новость за запуск — остальные дождутся своего
    # времени в следующих запусках workflow (см. post_at у каждого элемента).
    # FORCE_NOW игнорирует время ожидания — удобно для ручной проверки.
    for item in state["items"]:
        if item["posted"]:
            continue
        if now < datetime.fromisoformat(item["post_at"]) and not FORCE_NOW:
            continue

        try:
            if not item.get("image"):
                raise ValueError("картинка не найдена")
            result = send_photo_to_telegram(item["image"], item["caption"])
        except Exception as e:
            print(f"[WARN] Пост без фото ({e}), отправляю текстом: {item['title'][:60]}")
            result = send_to_telegram(item["caption"])

        print(f"[OK] Отправлено ({item['title'][:60]}):", result.get("ok"))
        item["posted"] = True
        append_single_to_log(item)
        save_queue(state)
        break  # одна новость за запуск, дальше просто выходим

    all_posted = all(i["posted"] for i in state["items"])

    if all_posted and not state["history_posted"]:
        history_event = fetch_tech_history_event()
        history_blurb = generate_history_blurb(history_event)
        history_block = build_history_block(history_event, history_blurb)
        if history_block:
            send_to_telegram(history_block)
            print("[OK] Рубрика истории отправлена")
        state["history_posted"] = True
        save_queue(state)

    if all_posted and state["history_posted"] and not state["footer_posted"]:
        send_to_telegram(FOOTER_TEXT)
        state["footer_posted"] = True
        save_queue(state)
        prune_log()

    # 1-го числа каждого месяца, после того как весь сегодняшний план публикаций
    # завершён — итог месяца по реальным данным + опрос на этой основе.
    # Флаг monthly_sent защищает от повторной отправки при следующих часовых запусках.
    if (
        now.day == 1
        and all_posted
        and state["footer_posted"]
        and not state["monthly_sent"]
    ):
        recap_result, poll_result = send_monthly_recap_and_poll()
        print("[OK] Итог месяца отправлен:", recap_result.get("ok"))
        print("[OK] Ежемесячный опрос отправлен:", poll_result.get("ok"))
        state["monthly_sent"] = True
        save_queue(state)


if __name__ == "__main__":
    main()
