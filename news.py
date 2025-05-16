from newsapi import NewsApiClient
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, date, timedelta
import time

load_dotenv()

def get_news_metadata(company: str, start_time: datetime = None, end_time:datetime = None):
    
    NEWS_API_KEY = os.getenv('NEWS_API_KEY')
    if not NEWS_API_KEY:
        raise Exception("Missing NEWS_API_KEY in .env file")
    date_format="YYYY-MM-DDTHH:MM:SS"
    if end_time==None:
        end_time = datetime.now(timezone.utc) - timedelta(days=1)
    if start_time==None:
        start_time = end_time - timedelta(hours=1)
    start_time_iso_str=start_time.isoformat()[:len(date_format)]
    end_time_iso_str=end_time.isoformat()[:len(date_format)]

    print(f"start:{start_time_iso_str} | end: {end_time_iso_str}")
    
    newsapi = NewsApiClient(api_key=NEWS_API_KEY)

    articles = newsapi.get_everything(q=f"{company} stock",
                                      from_param=start_time_iso_str,
                                      to=end_time_iso_str,
                                      language="en",
                                      sort_by="relevancy",
                                      page_size=100)
    
    print(f"Fetched Articles: {len(articles['articles'])} between {start_time_iso_str} and {end_time_iso_str}")
    news_metadata=[]
    for article in articles['articles']:
        curr_article_data={
            "title":article["title"],
            "description":article["description"],
            "url":article["url"]
        }
        news_metadata.append(curr_article_data)
    return news_metadata

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

def get_news_with_text(company, limit=10):
    metadata = get_news_metadata(company)
    full_articles = []
    index=0
    for article in metadata:
        if index>=limit:
            break
        print(f"\nScraping: {article['title']}")
        text = scrape_article_text(article['url'])
        full_articles.append({
            "title": article['title'],
            "url": article['url'],
            "text": text
        })
        index+=1

    return full_articles

def convert_news_array_to_text(news_data):
    text=""
    for article in news_data:
        text+=article["title"]
        text+=article["text"]
    return text

if __name__=="__main__":
    results = get_news_with_text("Apple")  # Change company name here

    for article in results:
        print("\n---")
        print("Title:", article["title"])
        print("URL:", article["url"])
        print("Text Snippet:", article["text"][:500])

    text=convert_news_array_to_text(results)
    print("FULL TEXT: ________________________")
    print(text)