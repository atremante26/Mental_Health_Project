from pipeline.ingestion import BaseIngestor
import os
from datetime import datetime, timezone, timedelta
import logging
import praw
import pandas as pd


# Configure logging
logger = logging.getLogger(__name__)

class RedditIngestor(BaseIngestor):
    def __init__(self):
        super().__init__()
        self.reddit = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=os.getenv("REDDIT_USER_AGENT")
        )
        self.subreddits = ["mentalhealth", "depression", "anxiety"]
    
    def load_data(self):
        try:
            posts = []
            for sub in self.subreddits:
                for post in self.reddit.subreddit(sub).hot(limit=50):
                    posts.append({
                        "subreddit": sub,
                        "title": post.title,
                        "score": post.score,
                        "created_utc": post.created_utc,
                        "url": post.url,
                        "selftext": post.selftext[:500],
                        "num_comments": post.num_comments
                    })

            last_week = datetime.now(timezone.utc) - timedelta(days=7)
            posts = [
                p for p in posts
                if datetime.fromtimestamp(p["created_utc"], tz=timezone.utc) > last_week
            ]
            return pd.DataFrame(posts)
        
        except Exception as e:
            logger.error(f"Reddit ingestion failed: {e}")
            raise 

    
    def process_data(self, df):
        df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s", utc=True)
        df["date"] = df["created_utc"].dt.strftime("%Y-%m-%d")
        return df[["subreddit", "date", "title", "selftext", "score", "num_comments"]].rename(
            columns={"selftext": "text", "num_comments": "comments"}
        )

if __name__ == "__main__":
    reddit_ingestor = RedditIngestor()
    reddit_ingestor.run("reddit", "reddit_suite", True, True)