import feedparser
import logging
import openai
import os
import hashlib
import sqlite3
import re
import requests
import csv
import schedule
import time
from dotenv import load_dotenv
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from bs4 import BeautifulSoup

# Загрузка переменных окружения
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
TELEGRAM_THREAD_ID = os.getenv("TELEGRAM_THREAD_ID")  # Если нужно отправлять в конкретную тему канала
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
RSS_URL = "https://rss.sciencedirect.com/publication/science/03603199"
DB_FILE = "ijohe_db.sqlite"

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Инициализация бота
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# Настройка OpenAI API
openai.api_key = OPENAI_API_KEY

def parse_rss():
    """
    Парсит RSS-ленту ScienceDirect.
    Из тега <description> извлекаются дата публикации (из строки с "Publication date:")
    и автор(ы) (из строки с "Author(s):").
    """
    logger.info(f"Загружаем RSS-ленту: {RSS_URL}")
    feed = feedparser.parse(RSS_URL)

    if feed.bozo:
        logger.error("Ошибка при парсинге RSS-ленты.")
        return []

    if not feed.entries:
        logger.warning("Нет статей в RSS-ленте.")
        return []

    articles = []
    # Для теста берем 5 статей. Уберите [:5], если нужно обрабатывать все.
    for entry in feed.entries[:5]:
        title = entry.get('title', 'Без названия')
        link = entry.get('link', '#')
        description_html = entry.get('description', '')
        soup = BeautifulSoup(description_html, 'html.parser')
        p_tags = soup.find_all('p')

        publication_date = "Неизвестно"
        authors = "Неизвестны"

        for p in p_tags:
            text = p.get_text().strip()
            lower_text = text.lower()
            if lower_text.startswith("publication date:"):
                publication_date = text.split(":", 1)[1].strip()
            elif lower_text.startswith("author(s):"):
                authors = text.split(":", 1)[1].strip()

        articles.append({
            'title': title,
            'link': link,
            'published_date': publication_date,
            'authors': authors,
            'annotation': 'Аннотация отсутствует.',  # По умолчанию; обновится после парсинга
            'description': description_html
        })

    logger.info(f"Получено {len(articles)} статей из RSS-ленты.")
    return articles

def fetch_annotation(article_url):
    """
    Получает HTML-страницу по ссылке статьи на ScienceDirect.
    Возвращает текст страницы или "" в случае ошибки.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.sciencedirect.com/"
    }
    try:
        response = requests.get(article_url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.error(f"Ошибка при получении страницы: {e}")
        return ""

def clean_annotation(html_text):
    """
    Ищем аннотацию в разных вариантах контейнеров.
    """
    soup = BeautifulSoup(html_text, "html.parser")

    # Сначала пробуем найти старые варианты
    abstract_div = soup.find("div", class_="Abstracts")
    if not abstract_div:
        abstract_div = soup.find("div", class_="svAbstract")

    # Если не нашли, пробуем «новый» вариант
    if not abstract_div:
        abstract_div = soup.find("div", class_="abstract author")

    if abstract_div:
        text = abstract_div.get_text(separator=" ", strip=True)
        # Удаляем всё, что идёт после "Graphical abstract", если есть
        if "Graphical abstract" in text:
            text = text.split("Graphical abstract")[0].strip()
        # Сжимаем повторяющиеся пробелы
        text = re.sub(r'\s+', ' ', text)
        return text

    return "Annotation not found."

def translate_title_openai(eng_title: str) -> str:
    """
    Переводит заголовок статьи на русский язык через GPT-4 (или gpt-3.5-turbo).
    """
    if not eng_title or eng_title == "No Title":
        return "Нет заголовка"

    try:
        completion = openai.chat.completions.create(
            model="gpt-4o",  # Или "gpt-3.5-turbo", если нет GPT-4
            messages=[
                {
                    "role": "system",
                    "content": "Ты — профессиональный переводчик. Переведи заголовок статьи на русский язык, сохрани стиль и смысл."
                },
                {
                    "role": "user",
                    "content": eng_title
                }
            ],
            temperature=0
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        print("Ошибка при обращении к OpenAI:", e)
        return eng_title

def translate_annotation_openai(eng_annotation: str) -> str:
    """
    Переводит аннотацию на русский язык.
    Если аннотации нет, возвращает "Аннотация не найдена."
    """
    if not eng_annotation or eng_annotation == "Annotation not found.":
        return "Аннотация не найдена."

    try:
        completion = openai.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ты — профессиональный аналитик и переводчик. "
                        "Выбери из текста только текст аннотации и переведи аннотацию на русский язык. "
                        "Раздел Highlights не нужен совсем, не переводи его и не включай в окончательный текст. "
                        "Нужен только текст аннотации, переведенный на русский язык. Не пиши слово Аннотация вначале."
                    )
                },
                {
                    "role": "user",
                    "content": eng_annotation
                }
            ],
            temperature=0.3
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        print("Ошибка при обращении к OpenAI:", e)
        return eng_annotation

def save_to_db(article):
    """
    Сохраняет статью в базу данных.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            hash TEXT PRIMARY KEY,
            title_ru TEXT,
            annotation_ru TEXT,
            authors TEXT,
            published_date TEXT,
            url TEXT
        )
    """)
    cursor.execute("INSERT OR IGNORE INTO articles VALUES (?, ?, ?, ?, ?, ?)",
                   (article['hash'], article['title_ru'], article['annotation_ru'],
                    article['authors'], article['published_date'], article['link']))
    conn.commit()
    conn.close()

def is_article_new(article_hash: str) -> bool:
    """
    Проверяет, есть ли статья с данным хэшем в базе.
    Возвращает True, если статья новая, иначе False.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            hash TEXT PRIMARY KEY,
            title_ru TEXT,
            annotation_ru TEXT,
            authors TEXT,
            published_date TEXT,
            url TEXT
        )
    """)
    conn.commit()
    cursor.execute("SELECT 1 FROM articles WHERE hash=?", (article_hash,))
    exists = cursor.fetchone() is not None
    conn.close()
    return not exists

def sanitize_for_telegram(text: str) -> str:
    """
    Удаляем теги <sub>...</sub> и <sup>...</sup>,
    т.к. Telegram в режиме HTML не поддерживает их.
    """
    text = text.replace("<sub>", "").replace("</sub>", "")
    text = text.replace("<sup>", "").replace("</sup>", "")
    return text

def publish_to_telegram(article):
    """
    Публикует статью в Telegram-канале.
    """
    message = (
        f"<b>{article['title_ru']}</b>\n"
        f"Дата публикации: {article['published_date']}\n"
        f"Автор(ы): {article['authors']}\n\n"
        f"{article['annotation_ru']}\n\n"
    )
    markup = InlineKeyboardMarkup()
    btn = InlineKeyboardButton(text="Читать далее", url=article['link'])
    markup.add(btn)

    try:
        if TELEGRAM_THREAD_ID and TELEGRAM_THREAD_ID.strip():
            bot.send_message(
                TELEGRAM_CHANNEL_ID,
                message,
                parse_mode="HTML",
                reply_markup=markup,
                reply_to_message_id=int(TELEGRAM_THREAD_ID)
            )
        else:
            bot.send_message(
                TELEGRAM_CHANNEL_ID,
                message,
                parse_mode="HTML",
                reply_markup=markup
            )
        logger.info("Сообщение опубликовано в Telegram-канале.")
    except Exception as e:
        logger.error(f"Ошибка публикации в Telegram-канале: {e}")

def main():
    """
    1. Парсинг RSS
    2. Проверка и перевод новых статей
    3. Сохранение в БД
    4. Публикация в Telegram
    """
    articles = parse_rss()
    for article in articles:
        # Генерация уникального хэша
        article['hash'] = hashlib.md5(f"{article['title']}{article['link']}".encode()).hexdigest()

        # Если уже есть, пропускаем
        if not is_article_new(article['hash']):
            logger.info(f"Статья с хэшем {article['hash']} уже существует. Пропускаем публикацию.")
            continue

        # Перевод заголовка
        article['title_ru'] = translate_title_openai(article['title'])

        # Скачиваем страницу, ищем аннотацию, переводим
        page_html = fetch_annotation(article['link'])
        raw_annotation = clean_annotation(page_html)
        article['annotation_ru'] = translate_annotation_openai(raw_annotation)

        # Удаляем <sub>/<sup> из заголовка и аннотации перед сохранением в БД
        article['title_ru'] = sanitize_for_telegram(article['title_ru'])
        article['annotation_ru'] = sanitize_for_telegram(article['annotation_ru'])

        # Сохранение в БД
        save_to_db(article)

        # Публикуем в Telegram
        publish_to_telegram(article)
        logger.info(f"Обработана новая статья: {article['title_ru']}")

def export_db_to_csv():
    """
    Выгружает всю таблицу articles в CSV.
    """
    filename = "ijohe_pub.csv"
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            hash TEXT PRIMARY KEY,
            title_ru TEXT,
            annotation_ru TEXT,
            authors TEXT,
            published_date TEXT,
            url TEXT
        )
    """)
    conn.commit()

    cursor.execute("SELECT hash, title_ru, annotation_ru, authors, published_date, url FROM articles")
    rows = cursor.fetchall()
    conn.close()

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["Hash", "Russian Title", "Russian Annotation", "Authors", "Publication Date", "URL"])
        for row in rows:
            writer.writerow(row)
    return filename

def send_csv_to_telegram():
    """
    Экспортируем БД в CSV и отправляем файл в Telegram.
    """
    filename = export_db_to_csv()
    try:
        with open(filename, "rb") as f:
            if TELEGRAM_THREAD_ID and TELEGRAM_THREAD_ID.strip():
                bot.send_document(TELEGRAM_CHANNEL_ID, f, caption="Свод публикаций IJOHE (CSV)", message_thread_id=int(TELEGRAM_THREAD_ID))
            else:
                bot.send_document(TELEGRAM_CHANNEL_ID, f, caption="Свод публикаций IJOHE (CSV)")
        logger.info("CSV-файл отправлен в Telegram!")
    except Exception as e:
        logger.error("Ошибка при отправке CSV-файла: " + str(e))

# Планировщик заданий
# 1) Проверяем RSS каждую минуту
schedule.every(1).minutes.do(main)
# 2) Отправляем CSV по субботам в 02:50 (пример)
schedule.every().saturday.at("17:00").do(send_csv_to_telegram)

if __name__ == "__main__":
    logger.info("Бот запущен. Ожидание задач...")
    while True:
        schedule.run_pending()
        time.sleep(1)
