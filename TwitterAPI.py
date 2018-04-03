import datetime
import redis
import csv
import pandas as pd


# Tweet Class
class Tweet:
    #Member declarations
    userID = 0
    tweetTime = ''
    tweet = ""
    tweetID = 0

    def get_userID(self):
        return self.userID

    def get_tweetTime(self):
        return self.tweetTime

    def get_tweet(self):
        return self.tweet

    def set_userID(self, userID):
        self.userID = userID
        return self.userID

    def set_tweetTime(self, time):
        self.tweetTime = time
        return self.tweetTime

    def set_tweet(self, tweetText):
        self.tweet = tweetText
        return self.tweet

    def get_tweetID(self):
        return self.tweetID

    def set_tweetID(self, twID):
        self.tweetID = twID


# Twitter API interface
class TwitterAPI:
    def postTweet(self, tweet):
        pass

    def addFollower(self, follwerID, userID):
        pass

    def getTimeline(self, userID, numTweets):
        pass

    def getFollowers(self, userID):
        pass

# Redis implements TwitterAPI
class RedisTwitterAPI(TwitterAPI):

    # create redis connection here
    r = redis.StrictRedis(host='localhost', port=6379, db=0)

    def postTweet(self, t):
        tweet = t.get_tweet()
        tweet = tweet[1:-1]
        user = t.get_userID()
        user = user[1:-1]
        tt = t.get_tweetTime()
        tt = tt[1:-1]
        tid = t.get_tweetID()
        tid = tid[1:-1]

        #hmset userid:postid user_id $owner_id time $time body "I'm having fun with Retwis"

        keyname = str(user) + ":" + str(tid)
        values = {'user_id': user, 'time': tt, 'tweet': tweet}

        self.r.hmset(keyname, values)

        #strategy 2
        #getFollowers of user
        followers = self.getFollowers(user)
        #addToTimelime for followers
        for f in followers:
            #f is followerID
            self.addToTimeline(f, tweet, tid)

        return True

    # Adds follower and following
    def addFollower(self, followerID, userID):

        # set
        # sADD followers:userID  followerID
        # sadd following:followerID userID

        key1 = 'followers:' + userID
        key2 = 'following:' + followerID

        self.r.sadd(key1, followerID)
        self.r.sadd(key2, userID)

        #key = followerID + 'follows:' + userID
        #self.r.sadd(key,)

        return True

    # Strategy 1: getTimeline
    def getTimeline(self, userID, numTweets):
        #query
        #keys userid:*
        #iterate through returned list

        key2 = 'following:' + str(userID)
        #print(key2)
        following = self.r.smembers(key2)
        #print(following)
        timelineTweets = list()
        timelineTimes = list()
        timelineUserID = list()


        # for each user 'userID' is following
        for f in following:
            # get userID and matching key from tweet hash
            #print(f)
            key = str(f)
            key = key[2:-3]
            pattern = '"' + "\\" + '"' + key + "\\" + '":*"'
            pattern2 = str(key) + ":*"
            pattern3 = '"' + str(key) + '":*"'
            #print(pattern)
            #print(pattern2)
            #print(pattern3)
            list_of_keys = self.r.keys(pattern2)
            #print(list_of_keys)

            list_of_userid = list()
            list_of_tweets = list()
            list_of_times = list()

            # for each key in list of keys
            for k in list_of_keys:
                # get tweet values
                #print(k)
                key = str(k)
                key = key[2:-1]
                #print(key)
                list_of_userid.append(self.r.hget(k, 'user_id'))
                list_of_tweets.append(self.r.hget(k, 'tweet'))
                list_of_times.append(self.r.hget(k, 'time'))

            timelineTweets.extend(list_of_tweets)
            timelineTimes.extend(list_of_times)
            timelineUserID.extend(list_of_userid)

        timeline_df = pd.DataFrame({'user_id': timelineUserID, 'tweet': timelineTweets, 'time': timelineTimes})
        timeline_df.sort_values(by=['time'], ascending=True)

        return timeline_df.iloc[0:numTweets, ]

    # Strategy2: gets folloers
    def getFollowers(self, userID):

        # used in strategy 2

        key1 = 'followers:' + userID + '.0'
        followers = self.r.smembers(key1)

        return followers


    # Strategy2: add tweet to timeline of given user
    def addToTimeline(self, userID, t, tid):

        # used for strategy 2
        # sorted set based on tweet ID (lazy way of sorting by time since earlier tweets have lower tweet_id)
        key1 = 'timeline:' + str(userID)[2:-1]
        values = {"user_id": userID, "tweet": t, "tweet_id": tid}
        self.r.sadd(key1, values)
        return True


class TwitterTester:

    api = RedisTwitterAPI()

    # Reads in & adds followers
    with open('followers.csv') as csvfile:

        followerreader = csv.reader(csvfile, delimiter='\n')
        for row in followerreader:
            # print(row)
            user = row[0].split(';')[0]
            # print(user)
            following = row[0].split(';')[1]
            api.addFollower(user, following)

    print(datetime.datetime.now())

    # Reads in & adds tweets
    with open('tweets.csv') as tweetfile:

        tweetreader = csv.reader(tweetfile, delimiter='\n')
        for row in tweetreader:
            tweet = Tweet()
            tweetline = row[0].split(',')
            #print(tweetline)
            tweetID = tweetline[0].split(':')[1]
            userID = tweetline[1].split(':')[1]
            tweetTS = tweetline[2].split(':')[1] + tweetline[2].split(':')[2] + tweetline[2].split(':')[3]
            tweetText = tweetline[3].split(':')[1]

            tweet.set_tweetID(tweetID)
            tweet.set_tweet(tweetText)
            tweet.set_userID(userID)
            tweet.set_tweetTime(tweetTS)

            api.postTweet(tweet)

    print(datetime.datetime.now())

    #strategy 1
    #read: get timeline 100 tweets / sec
    #write: ~8k tweets/sec -> 360k tweets in 50 seconds
    #print(api.getTimeline(676.0, 1000))

    #Strategy2
    #read: getting timeline is almost instantaneous : 1000 tweets/sec
    #write: ~4k tweets/sec
    #print(api.r.smembers('timeline:676.0'))