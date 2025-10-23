import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

api_key = os.getenv('NEWS_API_KEY')
base_url = 'https://newsapi.org/v2/everything'

# Test query - last 7 days of mental health articles
params = {
    'q': '("mental health" OR anxiety OR depression OR "suicide prevention")',
    'language': 'en',
    'sortBy': 'publishedAt',
    'from': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
    'to': datetime.now().strftime('%Y-%m-%d'),
    'pageSize': 10,  # Just 10 articles for testing
    'apiKey': api_key
}

response = requests.get(base_url, params=params)
print(f"Status: {response.status_code}")

data = response.json()
print(f"\nTotal results: {data.get('totalResults', 0)}")
print(f"\nArticles returned: {len(data.get('articles', []))}")

# Show first article structure
if data.get('articles'):
    print("\nFirst article structure:")
    article = data['articles'][0]
    print(f"  Title: {article.get('title')}")
    print(f"  Source: {article['source'].get('name')}")
    print(f"  Published: {article.get('publishedAt')}")
    print(f"  Description: {article.get('description')[:100]}...")
    print(f"  URL: {article.get('url')}")
    print(f"\n  All keys: {list(article.keys())}")