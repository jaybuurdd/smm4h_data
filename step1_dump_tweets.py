import yaml
from yaml import load, dump, Loader, Dumper
import tweepy
import time
from tweepy.error import TweepError
from Turtle import Job, ResultStore, LineReader

api_key = None
api_secret_key = None
with open('creds.yaml', 'r') as f:
    creds = load(f, Loader=Loader)
    api_key = creds['api_key']
    api_secret_key = creds['api_secret_key']


class Step1DumpTweets:
    def __init__(self, api_key, api_secret_key, progress_file, tid_file, dumpdir):
        self.api_key=api_key
        self.api_secret_key=api_secret_key
        self.progress_file = progress_file
        self.tid_file = tid_file
        self.dumpdir = dumpdir
        self.auth = tweepy.OAuthHandler(api_key, api_secret_key)
        self.api = tweepy.API(self.auth)
        self.rs = ResultStore(dumpdir, 2, 2)
        self.tj = Job(progress_file)

    def get_next_tids(self, linereader, n):
        res = []
        while True:
            ids = linereader.readlines(n -len(res))
            for tid in ids:
                if not self.tj.is_done(tid):
                    res.append(tid)
                else:
                    print('already have', tid)
            if linereader.iseof():
                break
            if len(res) == n:
                break
        return list(map(lambda x: int(x), res))

    def ex(self):
        lr = LineReader(self.tid_file, 'rt')
        while True:
            i = 0
            ids = self.get_next_tids(lr, 100)
            print('got next', len(ids))
            try:
                while True:
                    try:
                        tweets = self.api.statuses_lookup(ids)
                        for j in range(len(tweets)):
                            self.rs.dump(str(ids[j]),tweets[j])
                            self.tj.add_done(str(ids[j]))
                            break
                    except tweepy.RateLimitError:
                       print("sleeping due to rate limit")
                       time.sleep(15*60)
                       print("done sleeping")
            except TweepError:
                print('error ', tid, 'not found')
                print()
                self.tj.add_done(str(tid))
        lr.close()
        self.tj.close()

if __name__ == '__main__':
    s1 = Step1DumpTweets(api_key, api_secret_key, 'data/j1.txt', 'data/smm4h/remtids.tsv', 'data/tweetdump')
    s1.ex()

