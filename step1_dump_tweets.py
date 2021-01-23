import yaml
from yaml import load, dump, Loader, Dumper
import tweepy
import time
from tweepy.error import TweepError
from Turtle import Job, ResultStore
    
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

    def ex(self):
        i = 0
        with open(self.tid_file, 'r') as f:
            line = f.readline().strip() # get rid of header
            while True:
                line = f.readline().strip()
                if not line:
                    break
                i += 1
                tid = line.split('\t')[0]
                try:
                    if not self.tj.is_done(str(tid)):
                        while True:
                            try:
                                tweet = self.api.get_status(str(tid))
                                self.rs.dump(tid,tweet)
                                self.tj.add_done(str(tid))
                                print(i, '\t', tid, '\t', tweet.text)
                                print()
                                break
                            except tweepy.RateLimitError:
                                print("sleeping due to rate limit")
                                time.sleep(15*60)
                                print("done sleeping")
                except TweepError:
                    print('error ', tid, 'not found')
                    print()
                    self.tj.add_done(str(tid))

if __name__ == '__main__':
    s1 = Step1DumpTweets(api_key, api_secret_key, 'data/j1.txt', 'data/smm4h/remtids.tsv', 'data/tweetdump')
    s1.ex()

