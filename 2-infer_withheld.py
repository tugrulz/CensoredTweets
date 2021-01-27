import pandas as pd
metadata = pd.read_csv('./output/tweet_metadata.csv', dtype = {'linked_tweet':str, 'linked_user': str})
withheld = pd.read_csv('./output/tweets.csv')

metadata['withheld'] = False
metadata['linked_withheld'] = None

metadata.loc[metadata.id.isin(set(withheld.id)), 'withheld'] = True
metadata.loc[metadata.linked_tweet.notna(), 'linked_withheld'] = False
metadata.loc[metadata.linked_tweet.isin(set(withheld.id.astype(str))), 'linked_withheld'] = True

'''
Infer withheld with retweet trick
'''
print('Withheld, but linked is not withheld, and there is linked')
metadata.loc['user_withheld'] = False
metadata.loc[(metadata.withheld == True) & (metadata.linked_withheld == False), 'user_withheld'] = True
users_withheld = set(metadata[metadata.user_withheld == True].author_id)
metadata.loc[(metadata.author_id.isin(users_withheld)), 'user_withheld'] = True
metadata.to_csv('metadata_updated.csv', index = False)
