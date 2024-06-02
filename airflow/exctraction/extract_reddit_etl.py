import configparser
import datetime
import pandas as pd
import pathlib
import praw
import sys
import numpy as np
from validation import validate_input


# Read configuration file
parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
config_file = "configuration.conf"
parser.read(f"{script_path}/{config_file}")


# Config vars
SECRET = parser.get("reddit_config", "secret")
CLIENT_ID = parser.get("reddit_config", "client_id")


# Options for exctracting data from PRAW
SUBREDDIT = "dataengineering"
TIME_FILTER = "day"
LIMIT = None


# Fields that will be extracted from Reddit.
# Check PRAW documentation for additional fields.
# NOTE: if you change these, you'll need to update the create table
# sql query in the upload_aws_redshift.py file
POST_FIELDS = (
    "id",
    "title",
    "score",
    "num_comments",
    "author",
    "created_utc",
    "url",
    "upvote_ratio",
    "over_18",
    "edited",
    "spoiler",
    "stickied",
)


# Use command line argument as output file
# name and also store as column value
try:
    output_name = sys.argv[1]
except Exception as e:
    print(f"Error with file input. Error {e}")
    sys.exit(1)
date_dag_run = datetime.datetime.strptime(output_name, "%Y%m%d")


def main():
    """Exctract Reddit data and load to CSV"""
    validate_input(output_name)
    reddit_instance = api_connect()


def api_connect():
    """Connect to Reddit API"""
    try:
        instance = praw.Reddit(
            client_id=CLIENT_ID, client_secret=SECRET, user_agent="ETL API"
        )
        return instance
    except Exception as e:
        print(f"Unable to connect to API. Error: {e}")
        sys.exit(1)


def subreddit_posts(reddit_instance):
    """Create posts of object for Reddit instance"""
    try:
        subreddit = reddit_instance.subreddit(SUBREDDIT)
        posts = subreddit.top(time_filter=TIME_FILTER, limit=LIMIT)

        return posts
    except Exception as e:
        print(f"Unable to get redit posts. Error: {e}")
        sys.exit(1)