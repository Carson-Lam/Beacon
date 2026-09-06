import os
from dotenv import load_dotenv
import praw

load_dotenv()

reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
)
reddit.read_only = True

for submission in reddit.subreddit("wallstreetbets").hot(limit=5):
    print(submission.id, submission.title[:80])