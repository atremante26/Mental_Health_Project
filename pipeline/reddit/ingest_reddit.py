import praw
import json
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# Load credentials
reddit = praw.Reddit(
    client_id = os.getenv("REDDIT_CLIENT_ID"),
    client_secret = os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent = os.getenv("REDDIT_USER_AGENT")
)

# Target subreddits
subreddits = ["mentalhealth", "depression", "Anxiety"]

# Collect posts
all_posts = []
for sub in subreddits:
    for post in reddit.subreddit(sub).hot(limit=50):
        all_posts.append({
            "subreddit": sub,
            "title": post.title,
            "score": post.score,
            "created_utc": datetime.utcfromtimestamp(post.created_utc).isoformat(),
            "url": post.url,
            "selftext": post.selftext[:500]
        })

# Save to JSON
today = datetime.today().strftime("%Y-%m-%d")
out_path = f"data/reddit_posts_{today}.json"
with open(out_path, "w") as f:
    json.dump(all_posts, f, indent=2)