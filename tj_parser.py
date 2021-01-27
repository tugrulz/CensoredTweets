# from tweet_parser.tweet import Tweet
from datetime import datetime
import csv
import json
import os

class Parser:
    def __init__(self, write_folder, timeline, hydrated):
        self.folder = write_folder
        self.no_reply_quote = timeline # if tweets are collected by API.statuses_lookup, there is no reply and quote counts
        self.hydrated = hydrated # if tweets are hydrated later and at the same time, they do not have career
        self.num_of_tweets = 0
        self.num_of_retweets = 0
        self.num_of_quotes = 0
        self.num_of_replies = 0

    def create_writers(self):
        self.writer_dic = {}

        self.file_dic = {
            "tweet_metadata": "tweet_metadata.csv",
            "tweet_text": "tweet_text.csv",
            "entities_hashtag": "entities_hashtag.csv",
            "entities_mention": "entities_mention.csv",
            "entities_url": "entities_url.csv",
            "entities_media": "entities_media.csv",
            'twitter_user': 'twitter_user.csv',
            'user_timezone': 'user_timezone.csv',
            'user_profile': 'user_profile.csv',
            'user_tweet_career': 'user_tweet_career.csv',
            'network_retweet': "network_retweet.csv",
            'network_quote': "network_quote.csv",
            'network_reply': 'network_reply.csv',
            'geotagged_tweets': 'geotagged_tweets.csv',
            'deleted_tweets': 'deleted_tweets.csv',
            'withheld_tweets': 'withheld_tweets.csv'
        }

        self.fields_dic = {
            "tweet_metadata": ["id", "author_id", "created_at", "lang",
                               "tweet_type", "source", "source_link",
                               "retweet_count", "favorite_count", "quote_count", "reply_count",
                               "linked_tweet", "linked_user",
                               "sensitive", "access"],
            "tweet_text": ["id", "lang", "text"],

            "entities_hashtag": ["id", "hashtag"],
            "entities_mention": ["id", "mentioned_id", "mentioned_screen_name"],
            "entities_url" : ["id", "url", "host"],
            "entities_media" : ["id", "media_id", "type", "url"],

            "twitter_user" : ["id", "created_at", "lang", "name", "screen_name",
                              "location", "description", "url",
                              "followers_count", "friends_count", "favourites_count",
                              "listed_count", "statuses_count",
                              "protected", "verified", "default_profile", "default_profile_image",
                              "access"],

            'user_timezone': ["author_id", "tweet_id", "time_zone", "utc_offset", "access"],
            'user_profile': ["id", "profile_background_image_url", "profile_image_url", "profile_banner_url", "access"],
            'user_tweet_career': ["author_id", "tweet_id", "retweet_count",
                                  "favourite_count", "reply_count", "quote_count",
                                  "followers_count", "friends_count", "statuses_count", "access", 
                                  "screen_name", 'name', 'description', 'profile_image_url', 'user_lang', 'location', 'url'], # screen_name is new

            "network_retweet": ["id", "original_tweet_id", "retweeted_date", "author_id", "original_author_id", "screen_name", "original_screen_name"],
            "network_reply": ["id", "original_tweet_id", "replied_date", "author_id", "original_author_id", "screen_name", "original_screen_name"],
            "network_quote": ["id", "original_tweet_id", "quoted_date", "author_id", "original_author_id", "screen_name", "original_screen_name"],
            'geotagged_tweets': ["id", "country_code", "name", "place_type"],
            "deleted_tweets": ["id", "author_id", "timestamp_ms", "deletion_time"],
            'withheld_tweets': ["id", "country"]
        }

        if(self.no_reply_quote == True):
            self.fields_dic["tweet_metadata"].remove("quote_count")
            self.fields_dic["tweet_metadata"].remove("reply_count")
            self.fields_dic["user_tweet_career"].remove("reply_count")
            self.fields_dic["user_tweet_career"].remove("quote_count")

        for file, path in self.file_dic.items():
            fieldnames = self.fields_dic[file]
            self.writer_dic[file] = {}

            if (os.path.isfile(self.folder + path)):
                self.writer_dic[file]['f'] = open(self.folder + path, 'a', encoding='utf-8', newline='')
                self.writer_dic[file]['writer'] = csv.DictWriter(self.writer_dic[file]['f'], fieldnames=fieldnames)

            else:
                self.writer_dic[file]['f'] = open(self.folder + path, 'w', encoding='utf-8', newline='')
                self.writer_dic[file]['writer'] = csv.DictWriter(self.writer_dic[file]['f'], fieldnames=fieldnames)
                self.writer_dic[file]['writer'].writeheader()

    def close_writers(self):
        for file, dic in self.writer_dic.items():
            dic['f'].close()

    # recursively parse json object
    def parse_obj(self, json_obj):
        '''
        :param json_obj: dict
        :return:
        '''

        non_tweet = self.handle_non_tweet(json_obj)
        if (non_tweet):
            return

        json_obj = self.handle_old_data(json_obj) # for full_text - text conversion
        # tweet = Tweet(json_obj)
        tweet = json_obj
        self.parse_tweet(tweet, tweet['created_at'])

    def infer_tweet_type(self, tweet):
        if "retweeted_status" in tweet:
            return "retweet"
        elif "quoted_status" in tweet:
            return "quote"
        elif tweet["in_reply_to_status_id"] != None:
            return "reply"
        else:
            return "tweet"

    def convert_created_at(self, created_at):
        created_at = str(datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y').strftime('%Y-%m-%d %H:%M:%S'))
        return created_at

    def parse_tweet(self, tweet, access):
        '''

        :param tweet: json_obj
        :param access: datetime
        :return:
        '''

        tweet_type = self.infer_tweet_type(tweet)
        tweet['tweet_type'] = tweet_type
        tweet['created_at'] = self.convert_created_at(tweet['created_at'])

        if (tweet_type == 'tweet'):
            self.num_of_tweets += 1

        elif (tweet_type == 'retweet'):
            self.parse_tweet(tweet['retweeted_status'], access)
            self.add_retweet_link(tweet)
            self.num_of_retweets += 1

        # note that it only embeds one quote tweet
        elif (tweet_type == 'quote'):
            self.parse_tweet(tweet['quoted_status'], access)
            self.add_quote_link(tweet)
            self.num_of_quotes += 1

        elif (tweet_type == 'reply'):
            self.add_reply_link(tweet)
            self.num_of_replies += 1

        else:
            raise NotImplementedError

        self.parse_metadata(tweet, access)
        self.parse_user(tweet, access)
        self.parse_withheld(tweet, access)

        if("place" in tweet and tweet['place'] != None):
            self.parse_geotag(tweet)

        if (tweet['tweet_type'] != "retweet"):
            self.parse_text(tweet)
            self.parse_entities(tweet)

        if(not self.hydrated):
            self.parse_career(tweet, access)


    def write(self, table_name, rows):
        '''

        :param table_name:
        :param obj: list of rows (list)
        :return:
        '''
        fields = self.fields_dic[table_name]
        for row in rows:
            # row = ','.join([row[field] for field in fields])
            self.writer_dic[table_name]['writer'].writerow(row)



    def handle_non_tweet(self, json_obj):
        if("delete" in json_obj):
            self.parse_delete(json_obj)
            return True

        if("scrub_geo" in json_obj):
            return True

        if("limit" in json_obj):
            return True

        if('status_withheld' in json_obj):
            with open('become_withheld.json', 'a') as f:
                f.write(json.dumps(json_obj) + "\n")
            return True


    def handle_old_data(self, json_obj):
        if('text' not in json_obj):
            try:
                json_obj['text'] = json_obj['full_text']
            except:
                print("Neither text nor full_text exist on this json obj, there is a problem...")
                print(json_obj)
                exit()

            if ('quoted_status' in json_obj and 'text' not in json_obj['quoted_status']):
                json_obj['quoted_status']['text'] = json_obj['quoted_status']['full_text']

            if ('retweeted_status' in json_obj and 'text' not in json_obj['retweeted_status']):
                json_obj['retweeted_status']['text'] = json_obj['retweeted_status']['full_text']

                if ('quoted_status' in json_obj['retweeted_status'] and 'text' not in json_obj['retweeted_status']['quoted_status']):
                    json_obj['retweeted_status']['quoted_status']['text'] = json_obj['retweeted_status']['quoted_status']['full_text']

            return json_obj

        else:
            return json_obj



    def parse_delete(self, json_obj):
        json_obj = json_obj['delete']

        timestamp_ms = json_obj['timestamp_ms']
        deletion_time = datetime.utcfromtimestamp(int(timestamp_ms)/1000).strftime('%Y-%m-%d %H:%M:%S')

        id = str(json_obj['status']['id'])
        author_id = str(json_obj['status']['user_id'])

        rows = [{"id": id, "author_id": author_id, "timestamp_ms": timestamp_ms, "deletion_time": deletion_time}]

        self.write("deleted_tweets", rows)

    def parse_geotag(self, tweet):
        id = tweet['id']
        place = tweet['place']
        country_code = place['country_code']
        name = place['name']
        place_type = place['place_type']

        rows =  [{"id": id, "country_code": country_code, "name": name, "place_type": place_type}]

        self.write("geotagged_tweets", rows)

    def parse_text(self, tweet):
        id = tweet['id']

        text = tweet['text']
        if("extended_tweet" in tweet):
            text = tweet['extended_tweet']['full_text']

        text = text.replace('\r', ' ')
        text = text.replace('\n', ' ') # does more harm than benefit

        try:
        	lang = tweet['lang']
        except:
        	lang = 'unknown'

        rows =  [{"id": id, "text": text, "lang": lang}]

        self.write("tweet_text", rows)

    def parse_withheld(self, tweet, access):
        if('withheld_in_countries' in tweet and tweet['withheld_in_countries'] != None):
            liste = tweet['withheld_in_countries']

            rows = [{"id": tweet['id'], "country": country} for country in liste]

            self.write("withheld_tweets", rows)

    def parse_metadata(self, tweet, access):
        id = tweet['id']
        author_id = tweet['user']['id']

        try:
        	lang = tweet['lang']
        except:
        	lang = 'unknown'

        created_at = tweet['created_at']
        tweet_type = tweet['tweet_type']

        x = tweet['source']
        try:
        	source = x.split('>')[1].split('<')[0] if (type(x) == str and len(x) > 1) else ""
        except:
        	source = x

        try:
        	source_link = x.split('"')[1] if (type(x) == str and len(x) > 1) else ""
        except:
        	source_link = x

        try:
        	retweet_count = tweet['retweet_count']
        except:
        	retweet_count = -1

        try:
        	favourite_count = tweet['favorite_count']
        except:
        	favourite_count = -1

        if(self.no_reply_quote == False ):
            reply_count = tweet['reply_count']
            quote_count = tweet['quote_count']

        linked_tweet = ""
        linked_user = ""
        if(tweet_type == "quote"):
            linked_tweet = tweet['quoted_status']['id']
            linked_user = tweet['quoted_status']['user']['id']

        if(tweet_type == "reply"):
            linked_tweet = tweet['in_reply_to_status_id']
            linked_user = tweet['in_reply_to_user_id']

        if(tweet_type == "retweet"):
            linked_tweet = tweet['retweeted_status']['id']
            linked_user = tweet['retweeted_status']['user']['id']

        if("possibly_sensitive" in tweet):
            sensitive = tweet['possibly_sensitive']
        else:
            sensitive = False

        access = self.convert_created_at(access)

        dic = {"id": id, "author_id": author_id, "created_at": created_at, "lang": lang,
                "tweet_type": tweet_type, "source": source, "source_link": source_link,
               "retweet_count":retweet_count, "favorite_count":favourite_count,
               "linked_tweet": linked_tweet, "linked_user": linked_user,
               "sensitive": sensitive, "access": access}

        if (self.no_reply_quote == False): # note the reverse logic
            dic["quote_count"] = quote_count
            dic["reply_count"] = reply_count

        rows = [dic]

        self.write("tweet_metadata", rows)

    def parse_user(self, tweet, access):
        id = tweet['user']['id']
        created_at = tweet['user']['created_at']
        created_at = self.convert_created_at(created_at)
        lang = tweet['user']['lang']

        name = tweet['user']['name']
        name = name.replace('\n', ' ')
        name = name.replace('\r', ' ')
        
        screen_name = tweet['user']['screen_name']
        location = tweet['user']['location']
        if(location == None):
            location = ''
        location = location.replace('\n', ' ')
        location = location.replace('\r', ' ')
        description = tweet['user']['description']
        try:
            description = description.replace('\n', ' ')
            description = description.replace('\r', ' ')
        except:
            description = ""
        url = tweet['user']['url']

        followers_count = tweet['user']['followers_count']
        friends_count = tweet['user']['friends_count']
        favourites_count = tweet['user']['favourites_count']
        listed_count = tweet['user']['listed_count']
        statuses_count = tweet['user']['statuses_count']

        protected = tweet['user']['protected']
        verified = tweet['user']['verified']

        default_profile = tweet['user']['default_profile']
        default_profile_image = tweet['user']['default_profile_image']

        access = self.convert_created_at(access)

        self.parse_user_profile(tweet['user'], access)
        self.parse_timezone(tweet['id'], tweet['user'], access)

        rows = [{"id" : id, "created_at" : created_at, "lang" : lang, "name": name, "screen_name" : screen_name,
                "location" : location, "description" : description, "url" : url,
                "followers_count" : followers_count, "friends_count" : friends_count, "favourites_count" : favourites_count,
                "listed_count" : listed_count, "statuses_count" : statuses_count,
                "protected": protected, "verified": verified, "default_profile": default_profile,
               "default_profile_image": default_profile_image, "access": access}]

        self.write("twitter_user", rows)



    def parse_user_profile(self, user, access):
        author_id = user['id_str']
        profile_background_image_url = user['profile_background_image_url_https']
        profile_image_url = user['profile_image_url_https']
        profile_banner_url = user['profile_banner_url'] if 'profile_banner_url' in user else ''

        access = access

        rows = [{"id": author_id, "profile_background_image_url": profile_background_image_url,
                 "profile_image_url": profile_image_url, "profile_banner_url": profile_banner_url,
                 "access": access}]

        self.write("user_profile", rows)


    def parse_timezone(self, id, user, access):
        tweet_id = id
        author_id = user["id_str"]
        time_zone = user['time_zone']
        utc_offset = user['utc_offset']
        access = access

        if(time_zone != None or utc_offset != None):
            rows = [{"tweet_id": tweet_id, "author_id": author_id, "time_zone": time_zone,
                     "utc_offset": utc_offset, "access": access}]

            self.write("user_timezone", rows)

    def parse_career(self, tweet, access):
        author_id = tweet['user']['id']
        tweet_id = tweet['id']
        try:
        	retweet_count = tweet['retweet_count']
        except:
        	retweet_count = -1

        try:
        	favourite_count = tweet['favorite_count']
        except:
        	favourite_count = -1
        if (self.no_reply_quote == False):
            reply_count = tweet['reply_count']
            quote_count = tweet['quote_count']

        followers_count = tweet['user']['followers_count']
        friends_count = tweet['user']['friends_count']

        statuses_count = tweet['user']['statuses_count']
        access = self.convert_created_at(access)

        dic = {"author_id": author_id, "tweet_id":tweet_id, "retweet_count": retweet_count,
                "favourite_count": favourite_count, "followers_count": followers_count, "friends_count": friends_count,
               "statuses_count": statuses_count, "access": access}

        if (self.no_reply_quote == False):
            dic["reply_count"] = reply_count
            dic["quote_count"] = quote_count

        dic["screen_name"] = tweet['user']['screen_name']

        dic['name'] = tweet['user']['name'].replace('\n', ' ').replace('\r', ' ')

        try:
        	dic['description'] = tweet['user']['description'].replace('\n', ' ').replace('\r', ' ')
        except:
        	dic['description'] = ''

        dic['profile_image_url'] = tweet['user']['profile_image_url']
        dic['user_lang'] = tweet['user']['lang']

        try:
        	dic['location'] = tweet['user']['location'].replace('\n', ' ').replace('\r', ' ')
        except:
        	dic['location'] = ''

        try:
        	dic['url'] = tweet['user']['url'].replace('\n', ' ').replace('\r', ' ')
        except:
        	dic['url'] = ''




        rows = [dic]

        self.write("user_tweet_career", rows)


    def add_retweet_link(self, tweet):
        retweeted_tweet = tweet['retweeted_status']
        id = tweet['id']
        original_tweet_id = retweeted_tweet['id']
        author_id = tweet['user']['id']
        original_author_id = retweeted_tweet['user']['id']
        retweeted_date = tweet['created_at']
        screen_name = tweet['user']['screen_name']
        original_screen_name = retweeted_tweet['user']['screen_name']

        rows = [{"id": id, "original_tweet_id": original_tweet_id, "retweeted_date": retweeted_date,
                 "author_id": author_id, "original_author_id": original_author_id,
                 "screen_name": screen_name, "original_screen_name": original_screen_name}]

        self.write("network_retweet", rows)
        return

    def add_quote_link(self, tweet):
        quoted_status = tweet['quoted_status']
        id = tweet['id']
        original_tweet_id = quoted_status['id']
        author_id = tweet['user']['id']
        original_author_id = quoted_status['user']['id']
        quoted_date = tweet['created_at']
        screen_name = tweet['user']['screen_name']
        original_screen_name = quoted_status['user']['screen_name']

        rows = [{"id": id, "original_tweet_id": original_tweet_id, "quoted_date": quoted_date,
                 "author_id": author_id, "original_author_id": original_author_id,
                 "screen_name": screen_name, "original_screen_name": original_screen_name}]

        self.write("network_quote", rows)

        return

    def add_reply_link(self, tweet):
        id = tweet['id']
        original_tweet_id = tweet['in_reply_to_status_id']
        author_id = tweet['user']['id']
        original_author_id = tweet['in_reply_to_user_id']
        replied_date = tweet['created_at']
        screen_name = tweet['user']['screen_name']
        original_screen_name = tweet['in_reply_to_screen_name']

        rows = [{"id": id, "original_tweet_id": original_tweet_id, "replied_date": replied_date,
                 "author_id": author_id, "original_author_id": original_author_id,
                 "screen_name": screen_name, "original_screen_name": original_screen_name}]

        self.write("network_reply", rows)

    def parse_entities(self, tweet):
        if("extended_text" in tweet):
            tweet['entities'] = tweet['extended_tweet']['entities']
            tweet['extended_entities'] = tweet['extended_tweet']['extended_entities']

        entities = tweet['entities']

        if('extended_entities' in tweet):
            extended_entities = tweet['extended_entities']
        else:
            extended_entities = []


        hashtags = [el['text'] for el in entities['hashtags']]
        urls = [el['expanded_url'] for el in entities['urls']]

        hosts = [x.split('//')[1].split('/')[0] if x != None and len(x.split('//')) > 1 else '' for x in urls]

        mentions = [{'id': mention['id_str'], 'screen_name': mention['screen_name']} for mention in entities['user_mentions']]

        medias = []

        if (extended_entities != [] and "media" in extended_entities and len(extended_entities["media"]) > 0):
            for el in extended_entities["media"]:
                m_id = el['id_str']
                m_type = el['type']
                media_url = el['media_url_https']
                medias.append({'id': m_id, 'media_id': m_id, 'type': m_type, 'url': media_url})

        elif ("media" in entities and len(entities['media']) > 0):
            for el in entities["media"]:
                m_id = el['id_str']
                m_type = el['type']
                media_url = el['media_url_https']
                medias.append({'media_id': m_id, 'type': m_type, 'url': media_url})

        id = tweet['id']

        if(len(hashtags) > 0):
            rows = [{"id": id, "hashtag": hashtag} for hashtag in hashtags]
            self.write("entities_hashtag", rows)

        if(len(urls) > 0):
            rows = [{"id": id, "url": urls[i], "host": hosts[i]} for i in range(len(urls))]
            self.write("entities_url", rows)

        if(len(mentions) > 0):
            rows = [{"id": id, "mentioned_id": mentioned['id'], "mentioned_screen_name": mentioned["screen_name"]} for mentioned in mentions]
            self.write("entities_mention", rows)

        if(len(medias) > 0):
            rows = [{"id": id, "media_id": media['media_id'], "type": media["type"], "url": media["url"]} for media in medias]
            self.write("entities_media", rows)

    def get_stats(self):
        print("%d original tweets are read." % (self.num_of_tweets))
        print("%d retweets are read." % (self.num_of_retweets))
        print("%d quotes are read. " % (self.num_of_quotes))


