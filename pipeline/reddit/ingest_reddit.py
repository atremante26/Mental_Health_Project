import praw
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os

load_dotenv()

# Load credentials
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT")
)

# Target subreddits
subreddits = ["mentalhealth", "depression", "anxiety"]

# Collect posts
posts = []
for sub in subreddits:
    for post in reddit.subreddit(sub).hot(limit=50):
        posts.append({
            "subreddit": sub,
            "title": post.title,
            "score": post.score,
            "created_utc": post.created_utc,  # Unix timestamp
            "url": post.url,
            "selftext": post.selftext[:500],
            "num_comments": post.num_comments
        })

# Filter posts from the last 7 days
last_week = datetime.now(timezone.utc) - timedelta(days=7)
posts = [
    p for p in posts
    if datetime.fromtimestamp(p["created_utc"], tz=timezone.utc) > last_week
]

# Timestamped output filenames
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
os.makedirs("data", exist_ok=True)

# Save raw
with open(f"data/raw/reddit_raw_{today}.json", "w") as f:
    json.dump(posts, f, indent=2)

# Save processed
processed = [
    {
        "subreddit": p["subreddit"],
        "date": datetime.fromtimestamp(p["created_utc"], tz=timezone.utc).strftime("%Y-%m-%d"),
        "title": p["title"],
        "text": p["selftext"],
        "score": p["score"],
        "comments": p["num_comments"],
    }
    for p in posts
]

with open(f"data/processed/reddit_processed_{today}.json", "w") as f:
    json.dump(processed, f, indent=2)