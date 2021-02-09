# A Dataset of State-Censored Tweets
This GitHub repository includes (or will include) the documentation and the code for reproduction of the paper "A Dataset of State-Censored Tweets".

Dataset: https://zenodo.org/record/4439509#.YBHCNy2ZNQI

Paper: https://arxiv.org/abs/2101.05919


Attention! Please, if you use any data or code provided (including the parser), cite our paper (so I can safely finish PhD :))

MLA: Elmas, Tuğrulcan, Rebekah Overdorf, and Karl Aberer. "A Dataset of State-Censored Tweets." arXiv preprint arXiv:2101.05919 (2021).

Latex:
```

@article{elmas2021dataset,
  title={A Dataset of State-Censored Tweets},
  author={Elmas, Tu{\u{g}}rulcan and Overdorf, Rebekah and Aberer, Karl},
  journal={arXiv preprint arXiv:2101.05919},
  year={2021}
}
```

## Files
**tweets.csv** : All 583k censored tweets

**tweets_debiased.csv** : Debiased sample of tweets (Section 6.1)

**all_users.csv** : All users who are censored once at least once

**users.csv** : All 4301 users whose entire profile is censored

**users_inferred.csv** : 1931 extra users inferred to be the censored by the procedure described in Section 3.3 

**supplement.csv** : The supplementary tweet data. (Section 3.5)

## Getting Twitter Data

- To collect Twitter data, you can use twarc (https://github.com/DocNow/twarc). 

- Collecting tweets by ids is very easy, just call twarc hydrate ids.txt > output.json. ids.txt is a text file consisting of tweet ids (no header) 

## Reproduction

It is advisable to just get the tweets from Twitter instead of reproducing the paper as reproduction is time and memory consuming. In case you still one to do that:

- You need to download the archive data first (https://archive.org/details/twitterstream)

You can use wget for this (e.g. wget "https://archive.org/download/archiveteam-twitter-stream-2018-05/twitter-2018-05-02.tar")

- Unzip (only once, do not unzip .bz2s) and then use 1-mining.py to mine the bz2s for withheld tweets.

- Parse the resulting raw data using python parse.py. The raw data should go to input directory and the parsed data should go to output.

- The parsed data contains a couple self explanatory tables. You'll probably only need tweet_metadata.csv, tweet_text.csv and twitter_user.csv

- You can run 2-infer_withheld.py to infer which tweets were withheld in the past like in Section 3.2 of the paper.


