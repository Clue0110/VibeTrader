from newsapi import NewsApiClient
from datetime import datetime, timedelta, timezone
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
import os
import time

load_dotenv()
NEWS_API_KEY = os.getenv("NEWS_API_KEY")

def get_news_metadata(company: str, start_time: datetime = None, end_time: datetime = None):
    if not NEWS_API_KEY:
        raise Exception("Missing NEWS_API_KEY in .env file")

    if end_time is None:
        end_time = datetime.now(timezone.utc)
    if start_time is None:
        start_time = end_time - timedelta(days=7)  # get last 7 days of news

    date_format = "YYYY-MM-DDTHH:MM:SS"
    start_time_iso_str = start_time.isoformat()[:len(date_format)]
    end_time_iso_str = end_time.isoformat()[:len(date_format)]

    newsapi = NewsApiClient(api_key=NEWS_API_KEY)
    articles = newsapi.get_everything(
        q=f"{company} stock",
        from_param=start_time_iso_str,
        to=end_time_iso_str,
        language="en",
        sort_by="relevancy",
        page_size=100
    )

    print(f"Fetched Articles: {len(articles['articles'])} between {start_time_iso_str} and {end_time_iso_str}")
    return [{
        "title": a["title"],
        "description": a["description"],
        "url": a["url"]
    } for a in articles["articles"]]

def scrape_article_text(url):
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(2)
        paragraphs = driver.find_elements(By.TAG_NAME, "p")
        content = " ".join([p.text for p in paragraphs if p.text.strip() != ""])
        return content if content else "No readable content found."
    except Exception as e:
        return f"Error scraping content: {e}"
    finally:
        driver.quit()

def get_news_with_text(company):
    metadata = get_news_metadata(company)
    full_articles = []

    for article in metadata:
        print(f"\nScraping: {article['title']}")
        text = scrape_article_text(article['url'])
        full_articles.append({
            "title": article['title'],
            "url": article['url'],
            "text": text
        })

    return full_articles

if __name__ == "__main__":
    results = get_news_with_text("Google")  # Change company name here

    for article in results:
        print("\n---")
        print("Title:", article["title"])
        print("URL:", article["url"])
        print("Text Snippet:", article["text"][:500])

    import json

    with open("scraped_articles.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

    print("\nScraped data saved to scraped_articles.json ✅")