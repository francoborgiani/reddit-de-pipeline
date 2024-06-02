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
    posts = subreddit_posts(reddit_instance)
    exctracted_posts = extract_data(posts)
    transformed_posts = transform_data(exctracted_posts)
    
    load_to_csv(transformed_posts)


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

def extract_data(posts):
    """Exctract data to Pandas dataframe object"""
    items_list = []

    try:
        for submission in posts:
            to_dict = vars(submission)
            sub_dict = {field: to_dict[field] for field in POST_FIELDS}
            items_list.append(sub_dict)

        return pd.DataFrame(items_list)

    except Exception as e:
        print(f"Failed creating posts data frame. Error: {e}")
        sys.exit(1)


def transform_data(posts_df):
    """Some basic transformation of data. To be refactored at a later point."""
    try:
        # Convert epoch to UTC
        posts_df["created_utc"] = pd.to_datetime(posts_df["created_utc"], unit="s")
        # Fields don't appear to return as booleans (e.g. False or Epoch time). Needs further investigation but forcing as False or True for now.
        # TODO: Remove all but the edited line, as not necessary. For edited line, rather than force as boolean, keep date-time of last
        # edit and set all else to None.
        posts_df["over_18"] = np.where(
            (posts_df["over_18"] == "False") | (posts_df["over_18"] == False), False, True
        ).astype(bool)
        posts_df["edited"] = np.where(
            (posts_df["edited"] == "False") | (posts_df["edited"] == False), False, True
        ).astype(bool)
        posts_df["spoiler"] = np.where(
            (posts_df["spoiler"] == "False") | (posts_df["spoiler"] == False), False, True
        ).astype(bool)
        posts_df["stickied"] = np.where(
            (posts_df["stickied"] == "False") | (posts_df["stickied"] == False), False, True
        ).astype(bool)
        return posts_df
    except Exception as e:
        print(f"There was an error transforming the dataframe. Error: {e}")


def load_to_csv(extracted_data_df):
    """Save extracted data to CSV file in /tmp folder"""
    extracted_data_df.to_csv(f"/tmp/{output_name}.csv", index=False)


if __name__ == '__main__':
    main()