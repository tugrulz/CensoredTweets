import argparse
import os
import pandas as pd

def main():
    input_folder = './output/'

    try:
        write_folder = os.listdir(input_folder)[0]
    except:
        write_folder = ''

    parser = argparse.ArgumentParser(
        description="Create views.", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-i", "--input", dest="input_folder",
                        default=input_folder,
                        help="Name of the folder to read from, defaults to input")

    parser.add_argument("-w", "--write_folder", dest="write_folder",
                        default=write_folder,
                        help="Name of the file to put the tables, defaults to created_tables_firstfile")

    options = parser.parse_args()
    input_folder = "./" + options.input_folder + "/"
    write_folder = options.write_folder + "/"

    return input_folder + write_folder

def create_views(output_path):
    print("Creating views")
    # if(folder == '' or folder not in os.listdir(main_folder)):
    #     folders = [folder for folder in os.listdir(main_folder) if "created_tables" in folder]
    #     folder = folders[0] + "/"
    #     # view_folder = "views/"
    #     # os.mkdir(main_folder + folder + view_folder)
    main_folder = "./" + output_path.split('/')[1] + "/"
    folder = output_path.split('/')[2] + "/"

    # DIRECTED RETWEET GRAPH
    df = pd.read_csv(main_folder + folder + 'network_retweet.csv', dtype= {'author_id': str, 'original_author_id': str})
    created = df.groupby(by = ["author_id", "original_author_id"]).size().reset_index()
    created.columns = ["source", "target", "weight"]
    created.to_csv(main_folder + folder + "view_retweet_graph.csv", index = False)

    # USER STATS
    df = pd.read_csv(main_folder + folder + 'tweet_metadata.csv', dtype= {'author_id': str, 'id': str}, usecols = ['id', 'author_id'])
    created = df.groupby(by = ["author_id"]).size().reset_index()
    created.columns = ["author_id", "tw_num"]

    df = pd.read_csv(main_folder + folder + 'network_retweet.csv', dtype = {'author_id': str})
    created_2 = df.groupby(by = ["author_id"]).size().reset_index()
    created_2.columns = ["author_id", "do_rt_num"]

    df = pd.read_csv(main_folder + folder + 'network_retweet.csv', dtype = {'original_author_id': str})
    created_3 = df.groupby(by = ["original_author_id"]).size().reset_index()
    created_3.columns = ["author_id", "get_rt_num"]

    created = created.merge(created_2, how='outer', on='author_id')
    created = created.merge(created_3, how='outer', on='author_id')

    created = created.fillna(0)
    created.tw_num = created.tw_num.astype(int)
    created.do_rt_num = created.do_rt_num.astype(int)
    created.get_rt_num = created.get_rt_num.astype(int)

    created.to_csv(main_folder + folder + "view_user_stats.csv", index = False)

    # AUTHOR HASHTAG
    df = pd.read_csv(main_folder + folder + 'entities_hashtag.csv')
    df2 = pd.read_csv(main_folder + folder + 'tweet_metadata.csv', usecols = ["id", "author_id"])
    df = df.merge(df2, how = 'inner', on = 'id')
    df = df[['author_id', 'hashtag']]

    created = df.groupby(by = ["author_id", 'hashtag']).size().reset_index()
    created.columns = ["author_id", "hashtag", "count"]
    created.author_id = created.author_id.astype(str)
    created.to_csv(main_folder + folder + "view_author_hashtag.csv", index = False)

    # AUTHOR HASHTAG STATS
    df.author_id = df.author_id.astype(str)
    created.author_id = created.author_id.astype(str)
    created = created[['author_id']]
    created = created.set_index('author_id')
    # created = pd.DataFrame(columns = 'author_id', index = created.author_id)
    created['total_hashtags'] = df.groupby(by = ["author_id"]).size()
    created['different_hashtags'] = df.drop_duplicates().groupby(by = ["author_id"]).size()
    created = created.reset_index()
    created.columns = ['author_id', 'total_hashtags', 'different_hashtags']
    created = created.drop_duplicates()
    created.to_csv(main_folder + folder + "view_author_hashtag_stats.csv", index = False)

    # Activity View
    # tweet, retweet, deletion
    df = pd.read_csv(main_folder + folder + 'tweet_metadata.csv', dtype= {'author_id': str, 'id': str}, usecols = ['id', 'author_id', 'created_at', 'tweet_type'])

    df.loc[df['tweet_type'] == 'reply', 'tweet_type'] = 'tweet'
    df.loc[df['tweet_type'] == 'quote', 'tweet_type'] = 'tweet'

    try:
        dele = pd.read_csv(main_folder + folder + 'deleted_tweets.csv', dtype= {'author_id': str, 'id': str}, usecols = ['id', 'author_id', 'deletion_time'])
        dele = dele.rename({'deletion_time': 'created_at'}, axis = 1)

        dele['tweet_type'] = 'deletion'

        df = pd.concat([df, dele])
    except:
        print("No deleted_tweets.csv")

    df.to_csv(main_folder + folder + "view_activity.csv", index = False)


if __name__ == '__main__':
    output_path = main()
    create_views(output_path)