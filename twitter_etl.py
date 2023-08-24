import tweepy
import pandas as pd
import json
from datetime import datetime


def run_twitter_etl():
  access_key = "1692060582352977920-Sn87IregQcm5vnAJ59SWSt1hPt5m86" 
  access_secret = "msr4fG3Du7IqwVgctEPa7nbPav7OWpA4WFdFlKvYa7y3E" 
  consumer_key = "yIXvSHllXVfTwwdLu3DbF3Qps"
  consumer_secret = "OLzuNGKnJlFdzDm9pMRyJjCuPVr3EQmlSYweXzhWmVGqtZtt7B"



   # Twitter authentication
  auth = tweepy.OAuthHandler(access_key, access_secret)   
  auth.set_access_token(consumer_key, consumer_secret) 

    # # # Creating an API object 
  api = tweepy.API(auth)
  tweets = api.user_timeline(screen_name='@elonmusk', 
                            # 200 is the maximum allowed count
                            count=140,
                            include_rts = False,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )
  
  list = []
  for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)

  df = pd.DataFrame(list)
  df.to_csv("s3://bona-airflow-youtube-bucket/elonmusk_twitter_data.csv")