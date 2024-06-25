from pymongo import MongoClient
from abc import ABC, abstractmethod

import sys
import re
from datetime import datetime
import numpy as np
import pandas as pd

class ConnexionMongoDB:
    def __init__(self):
        self._client = MongoClient("mongodb://zlin:N3SpiL5SWq@884h.pulpss.fr:27017/if29")
        self._db = self._client["if29"]
         
    def userTweets(self, Vdata):
        self._project = self._db["projet"]
        self._users = self._db["userTweets"]

        # For test, only use the first 100 users
        self._cursor = self._users.find().limit(Vdata)

        # uid_tweets_map is a hashmap
        # {user_id : [tweetsObj1, tweetsObj2, ...]}
        uid_tweets_map = {}

        for user in self._cursor:
            tweets = []
            for idx in user["tweetIds"]:
                tweet = self._project.find_one({"_id": idx})
                tweets.append(tweet)
            uid_tweets_map[user["_id"]] = tweets

        self._client.close()

        # parse
        for uid in uid_tweets_map:
            # format datetime
            tweets = uid_tweets_map[uid]
            for t in tweets:
                t["created_at"] = datetime.strptime(t["created_at"], "%a %b %d %H:%M:%S %z %Y")

            # sorted by post time
            tweets = sorted(
                tweets,
                key=lambda t: t["created_at"],
            )

        user_id = []

        aps = self._calc_aps(uid_tweets_map)
        vps = []
        avg_retweets = []
        avg_urls = []
        avg_hashtags = []
        avg_mentions = []
        avg_text_lengths = []
        verified = []

        friends_count = []
        avg_favourites_count = []
        followers_count = []
        statuses_count = []
        ftweets = []
        ratio = []

        for uid in uid_tweets_map:
            user_id.append(uid)

            tweets = uid_tweets_map[uid]
            
            avg_retweets.append(self._calc_avg_retweet(tweets))
            avg_urls.append(self._calc_avg_url(tweets))
            avg_hashtags.append(self._calc_avg_hashtag(tweets))
            avg_mentions.append(self._calc_avg_mention(tweets))
            avg_text_lengths.append(self._calc_avg_text_length(tweets))
            verified.append(1 if tweets[-1]["user"]["verified"] else -1)
            vps.append(self._calc_vps(tweets))
            friends_count.append(tweets[-1]["user"]["friends_count"])
            avg_favourites_count.append(self._calc_avg_favourites_count(tweets))
            statuses_count.append(tweets[-1]["user"]["statuses_count"])
            followers_count.append(tweets[-1]["user"]["followers_count"])
            ftweets.append(self._f_tweets(tweets))
            ratio.append(0 if tweets[-1]["user"]["friends_count"] == 0 else tweets[-1]["user"]["followers_count"]/tweets[-1]["user"]["friends_count"])

        users_data = {
            "user_id":user_id,
            "avg_retweet":avg_retweets,
            "avg_url":avg_urls,
            "avg_hashtag": avg_hashtags,
            "avg_text_length":avg_text_lengths,
            "verified":verified,
            "ap":aps,
            "vp":vps,
            "friends_count":friends_count,
            "avg_favourites_count":avg_favourites_count,
            "statuses_count":statuses_count,
            "followers_count":followers_count
        }

        attributs_data = {
            "nbtweet": statuses_count,
            "ftweets": ftweets,
            "agressivity": aps,
            "visibility": vps,
            "avgUrls": avg_urls,
            "ratio": ratio,
            "avgMention": avg_mentions,
            "avgHashtag" : avg_hashtags
        }

        #dfUsers = pd.DataFrame(users_data)
        dfUsers = pd.DataFrame(attributs_data)

        return dfUsers
    
    def labelsWithUserData(self):
        self._collection = self._db.labelsWithUserData

        # Define the query
        self._query = {}
        self._projection = {
            "_id": 0,  # Exclude the "_id" field
            "created_at": 1,
            "favourites_count": 1,
            "followers_count": 1,
            "friends_count": 1,
            "label": 1,
            "url":1,
            "default_profile_image":1,
            "statuses_count": 1,
            "tweet_created_at": 1,
            "location":1,
            "verified":1,
            "name":1
        }

        # Execute the query and fetch the data
        self._cur = self._collection.find(self._query, self._projection).limit(1504)
        data = list(self._cur)
        df = pd.DataFrame(data)

        df['created_at'] = pd.to_datetime(df['created_at'])
        df['tweet_created_at'] = pd.to_datetime(df['tweet_created_at'])

        df['created_at'] = df['created_at'].apply(lambda x: x.timestamp())
        df['tweet_created_at'] = df['tweet_created_at'].apply(lambda x: x.timestamp())
        df['url'] = df['url'].astype('string')
        #df['url'][df['url'] == 'None'] = 0
        #df['url'] = df['url'].replace(np.nan, '0')
        df['url'] = df['url'].notna().astype(int)
        df['default_profile_image'] = df['default_profile_image'].astype(int)
        df['verified'] = df['verified'].astype(int)
        df['location'] = df['location'].notna().astype(int)
        df['name'] = df['name'].astype('string')
        df['name'] = df['name'].replace(np.nan, 'None')
        df['name'] = df['name'].apply(lambda x: x.isalpha()).astype(int)

        return df
    
    def _calc_aps(self, uid_tweets_map):
        # calc aggressive ap
        f_max = 350
        fts = []
        ffs = []
        # calc frequency of tweet
        for uid in uid_tweets_map:
            fts.append(self._f_tweets(uid_tweets_map[uid]))
            ffs.append(self._f_friends(uid_tweets_map[uid]))

        filtered_fts = [ft for ft in fts if ft != 0 and ft != -1]
        def_ft = np.median(filtered_fts) if len(filtered_fts) > 0 else 0
        fts = [def_ft if ft == -1 else ft for ft in fts]

        filtered_ffs = [ff for ff in ffs if ff != 0 and ff != sys.maxsize]
        def_ff = np.median(filtered_ffs) if len(filtered_ffs) > 0 else 0
        ffs = [def_ff if ff == sys.maxsize else ff for ff in ffs]

        aps = [(ff + ft) / f_max for ff, ft in zip(ffs, fts)]
        return aps

    def _calc_vps(self, tweets):
        if len(tweets) == 0:
            return 0
        cost_hashtag = 11.6
        cost_mention = 11.4
        tweet_length = 280 #140
        total_hashtags = 0
        total_mentions = 0
        for tweet in tweets:
            total_hashtags += tweet["text"].count('#')
            total_mentions += tweet["text"].count('@')
        return (self._calc_avg_hashtag(tweets) * cost_hashtag + self._calc_avg_mention(tweets) * cost_mention) / tweet_length

    def _f_tweets(self, tweets, default_ft=-1):
        if len(tweets) == 0:
            return 0
        elif len(tweets) == 1:
            return default_ft
        hours = (tweets[-1]["created_at"] - tweets[0]["created_at"]).total_seconds() / 3600
        if hours == 0:
            return 0
        return len(tweets) / hours


    def _f_friends(self, tweets, default_ff=sys.maxsize):
        if len(tweets) == 0:
            return 0
        elif len(tweets) == 1:
            return default_ff

        hours = (tweets[-1]["created_at"] - tweets[0]["created_at"]).total_seconds() / 3600
        if hours == 0:
            return 0
        cnt = tweets[-1]["user"]["friends_count"] - tweets[0]["user"]["friends_count"]
        return abs(cnt) / hours
    
    # avg_retweet
    # For each user: the average number of retweets per Tweet
    def _calc_avg_retweet(self, tweets):
        if(len(tweets) == 0):
            return 0
        return sum(tweet["retweet_count"] for tweet in tweets) / len(tweets)

    # avg_url
    # For each user: the average length of url
    def _calc_avg_url(self, tweets):
        if(len(tweets) == 0):
            return 0
        return sum(len(tweet["entities"]["urls"]) for tweet in tweets) / len(tweets)

    # avg_hashtag
    def _calc_avg_hashtag(self, tweets):
        if(len(tweets) == 0):
            return 0
        return sum(len(tweet["entities"]["hashtags"]) for tweet in tweets) / len(tweets)

    # avg_mentions
    def _calc_avg_mention(self, tweets):
        if(len(tweets) == 0):
            return 0
        return sum(len(tweet["entities"]["user_mentions"]) for tweet in tweets) / len(tweets) 

    # avg_text
    def _calc_avg_text_length(self, tweets):
        if(len(tweets) == 0):
            return 0
        total_words = 0
        for tweet in tweets:
            total_words += sum(1 for char in tweet)
        return total_words / len(tweets)

    # avg_favourites_count
    def _calc_avg_favourites_count(self, tweets):
        if(len(tweets) == 0):
            return 0
        return sum(tweet["user"]["favourites_count"] for tweet in tweets) / len(tweets)


