import yaml
from yaml import load, dump, Loader, Dumper
import tweepy
import pickle
import time
from tweepy.error import TweepError
from Turtle import Job, ResultStore, LineReader, Find
import pandas as pd

task_files = {
    'tsk1_trn': 'data/smm4h/smm4h-EMNLP-task1-trainingset.csv',
    'tsk2_trn_set1': 'data/smm4h/task2_trainingset1_download_form.txt',
    'tsk2_trn_set2': 'data/smm4h/task2_trainingset2_download_form.txt',
    'tsk2_trn_set3': 'data/smm4h/task2_trainingset3_download_form.txt',
    'tsk3_trn_set1': 'data/smm4h/task3_trainingset1_download_form.txt',
    'tsk3_trn_set2': 'data/smm4h/task3_trainingset2_download_form.txt',
    'tsk3_trn_set3': 'data/smm4h/task3_trainingset3_download_form.txt',
    'tsk3_trn_set4': 'data/smm4h/smm4th-EMNLP-task4-trainingset.tsv'
}

def strip_eol(lines):
    return list(map(lambda x: x.strip(), lines))

task_tids = {}
for data_set, file_name in task_files.items():
    with open(file_name, 'rt') as fp:
        task_tids[data_set] = pd.read_csv(file_name, delimiter='\t')

tweet_files = find.ex('data/tweetdump/*/*/*')
#rs = ResultsStore('data/tweetdump/',2,2)
created_at_arr = []
id_arr = []
text_arr = []
entities_arr = []
source_arr = []
source_url_arr = []
author_arr = []
is_quote_status_arr = []
retweet_count_arr = []
favorite_count_arr = []
lang_arr = []
for tweet_file in tweet_files:
    with open(tweet_file, "rb") as f:
        tweet = pickle.load(f)
        created_at_arr.append(tweet.created_at)
        id_arr.append(tweet.id)
        text_arr.append(tweet.text)
        source_arr.append(tweet.source)
        author_arr.append(tweet.author.id)
        is_quote_status_arr.append(tweet.is_quote_status)
        retweet_count_arr.append(tweet.retweet_count)
        favorite_count_arr.append(tweet.favorite_count)
        lang_arr.append(tweet.lang)

cols = {
    'id': id_arr,
    'text': text_arr,
    'source': source_arr,
    'author': author_arr,
    'is_quote_status': is_quote_status_arr,
    'retweet_count': retweet_count_arr,
    'favorite_count': favorite_count_arr,
    'lang': lang_arr
}

df = pd.DataFrame.from_dict(cols)
