import praw
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os
import boto3

load_dotenv()

# AWS S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION")
)

S3_BUCKET = "mental-health-project-pipeline"


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

# Get filenames
today = datetime.today().strftime("%Y-%m-%d")
raw_filename = f"reddit_raw_{today}.json"
processed_filename = f"reddit_processed_{today}.json"

# Processed data
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

# Save locally
os.makedirs("data", exist_ok=True)

with open(f"data/raw/{raw_filename}", "w") as f:
    json.dump(posts, f, indent=2)

with open(f"data/processed/{processed_filename}", "w") as f:
    json.dump(processed, f, indent=2)

# Upload to S3
s3.upload_file(f"data/{raw_filename}", S3_BUCKET, f"reddit-raw/{raw_filename}")
s3.upload_file(f"data/{processed_filename}", S3_BUCKET, f"reddit-processed/{processed_filename}")