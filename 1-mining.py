# You should download the archive and add the path of each year to paths list.

import os
import multiprocessing as mp
import json
import bz2

'''
EDIT HERE
'''
parallel = True # should the parsing run in parallel
processes = 20 # how many processes?
main_path = '?'

paths = [
    "%s/2020" % (main_path),
    "%s/2019" % (main_path),
    "..."
]

save_path = './input/'
'''
DONE EDITING
'''

def parse_withheld(file):
    date = '-'.join(file.split('/')[-5:-2])

    writer_withheld_tweets = open(save_path + date + "_withheldtweets.json", 'a')
    writer_withheld_users = open(save_path + date + "_withheldusers.json", 'a')
    writer_withheld_messages = open(save_path + date + "_withheldumessages.json", 'a')

    # print(file)
    bz_file = bz2.BZ2File(file)
    created_at = '-'.join(file.split('/')[-5:-2]) + " " + ':'.join(file.split('/')[-2:])[0:5] + ":00"

    # Check if the bz is broken
    try:
        bz_file_read = bz_file.read()
    except EOFError:
        return

    # get all lines in the bz file
    for line in bz_file_read.splitlines():

        # check if the json_obj in the line is broken
        try:
            json_obj = json.loads(line)
        except:
            continue

        #
        if('created_at' in json_obj):
            created_at = json_obj['created_at']

            # check tweet
            if ('withheld_in_countries' in json_obj):
                json_obj['linked'] = 'no' # no retweet or quote
                writer_withheld_tweets.write(json.dumps(json_obj) + "\n")

            # check user
            if ('withheld_in_countries' in json_obj['user']):
                json_obj['linked'] = 'no'
                writer_withheld_users.write(json.dumps(json_obj) + "\n")

            try:
                # handle retweet
                json_obj['retweeted_status']
                if('withheld_in_countries' in json_obj['retweeted_status']):
                    json_obj['linked'] = 'retweeted'
                    writer_withheld_tweets.write(json.dumps(json_obj) + "\n")

                if('withheld_in_countries' in json_obj['retweeted_status']['user']):
                    json_obj['linked'] = 'retweeted'
                    writer_withheld_users.write(json.dumps(json_obj) + "\n")

            except:
                # handle quote
                try:
                    json_obj['quoted_status']
                    if('withheld_in_countries' in json_obj['quoted_status']):
                        json_obj['linked'] = 'quoted'
                        writer_withheld_tweets.write(json.dumps(json_obj) + "\n")

                    if('withheld_in_countries' in json_obj['quoted_status']['user']):
                        json_obj['linked'] = 'quoted'
                        writer_withheld_users.write(json.dumps(json_obj) + "\n")

                except:
                    continue


        else:
            try:
                # if the json object does not have a "created_at" then it should be a withheld message
                json_obj['withheld_in_countries']
                # store the approximate time of the withheld message, which is the creation time of the previous tweet
                json_obj['approx_time'] = created_at
                writer_withheld_messages.write(json.dumps(json_obj) + "\n")

            except:
                continue

    writer_withheld_messages.close()
    writer_withheld_tweets.close()
    writer_withheld_users.close()


new_paths = []
for path in paths:
    months = os.listdir(path)
    for month in months:
        new_paths.append(path + "/" + month)

paths = new_paths
print(paths)

if(parallel):
    pool = mp.Pool(processes)
    from datetime import datetime
else:
    pool = None

start = datetime.now()

for path in paths:
    files = []
    for r, d, f in os.walk(path):
        for file in f:
            if file.endswith('.bz2'):
                files.append(os.path.join(r, file))

    print(path)
    print(len(files))

    if(parallel):
        pool.map(parse_withheld, [file for file in files])
    else:
        for file in files:
            parse_withheld(file)

pool.close()
pool.join()

end = datetime.now()

print(start)
print(end)











