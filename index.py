import json
import pandas as pd
from db_connection import engine, db_connection
from tw_etl_progress_bar import progress_bar



etl_progress = 0
etl_complete = 100

progress_bar(etl_progress, etl_complete, description="Start the ETL")

tweets_source_filename = 'query2_ref.txt'
with open(tweets_source_filename) as txtFile:
    lines =  txtFile.readlines()
      
def get_list_of_unclean_tweets(data):
    _all_data = []
    _no_json_unparsable_record = 0

    for line in data:
        try:
            _all_data.append(json.loads(line))
        except:
            _no_json_unparsable_record= _no_json_unparsable_record + 1
        progress_bar(10, etl_complete, description="Some records cannnot be parse")
    return _all_data


#Transform our unclean tweets data list to the dataframe
_tweets_data_unclean_df = pd.DataFrame(get_list_of_unclean_tweets(lines))

def verify_user_id(id, id_str):
        try:
            return (pd.notna(id) & isinstance(id, int)) or (pd.notna(id_str) & id_str.isnumeric())
        except:
            return False



def assign_tweet_type(tweet):
    if verify_user_id(tweet['in_reply_to_user_id'], tweet['in_reply_to_user_id_str']):
        return 'reply'
    elif pd.notna(tweet['retweeted_status']):
        return 'retweet'
    else:
        return 'no_type'

def assign_id(id, id_str):
    try:
        if isinstance(id, int) and id > 0:
            return id
        else:
            return int(id_str)
    except:
        return 0
def assign_user(tweet):
    try: 
        user = tweet['user']
        return assign_id(user.get('id'), user.get('id_str'))
    except:
        return 0

#Tweets cleaner
def transform_and_clean_tweets_data(tweets_data):
    

    #Filter out tweets with no id and no id_str
    tweets_data = tweets_data[hasattr(tweets_data,'id') and tweets_data.id.notnull() | (hasattr(tweets_data,'id_str') and tweets_data.id_str.notnull())]

    #Filter out tweets with no created_at
    tweets_data = tweets_data[hasattr(tweets_data,'created_at') and tweets_data.created_at.notnull()]

    #Filter out tweets with no text
    tweets_data = tweets_data[tweets_data.text.notnull()]

    #filter out tweets in language other than the ones provided in the TOR
    _accepted_langs = ['ar', 'en', 'fr', 'in', 'pt', 'es', 'tr', 'ja']
    tweets_data = tweets_data.apply(lambda row: row[tweets_data['lang'].isin(_accepted_langs)])
    # =>>>> same as _tweets_data_df = _tweets_data_df.query('lang in @accepted_langs')

    #filter out tweets with no hashtag
    tweets_data = tweets_data[tweets_data.entities.apply(lambda x: len(x['hashtags']) >=1)]

    #filter out tweets with no author (user has no id or id_str)
    tweets_data = tweets_data[tweets_data.user.apply(lambda x: verify_user_id(x['id'], x['id_str']))]

    #Add tweet type (a tweet is reply if in_reply_to_user_id is not null, and is retweet if retweeted_status is not null)
    tweets_data['type'] = tweets_data.apply(lambda x : assign_tweet_type(x), axis=1)

    tweets_data['user_id'] = tweets_data.apply(lambda row:  assign_user(row), axis=1)

    #Let's filter out all tweets that are not either reply or retweet
    tweets_data = tweets_data.apply(lambda row: row[tweets_data['type'].isin(['reply','retweet'])])

    return tweets_data

def sort_tweeter_data_df(tweets_data):
     #>>>>>>We should sort out dataframe in descending order so that we meet the requirement mentioned in the TOR of having most recent data.<<<<<<
    #Let's first convert the created_at column in the Timestamp data type
    tweets_data['created_at'] = pd.to_datetime(tweets_data['created_at'])
    #Then sort in descending order
    tweets_data.sort_values(by='created_at', ascending=False, inplace=True)

    return tweets_data

def get_hashtags(hashtags_list): 
        hashtags_list_str= ""
        for i in hashtags_list:
            hashtags_list_str += i['text']+", "
        return hashtags_list_str




def add_contacted_user(tweets):
    col_count = len(tweets.columns)
    _reply_tweets = tweets[tweets['type']=='reply']
    _retweeted_tweets=tweets[tweets['type']=='retweet']
    _reply_tweets.insert(loc=col_count, column='contact_user', value=_reply_tweets.apply(lambda x: assign_id(x['in_reply_to_user_id'], x['in_reply_to_user_id_str']), axis=1))
   
    _retweeted_tweets.insert(loc=col_count, column='contact_user', value=_retweeted_tweets.retweeted_status.apply(lambda x:  assign_user(x)))
   
    _tweets= pd.concat([_reply_tweets ,_retweeted_tweets])
    _tweets = sort_tweeter_data_df(_tweets)
    return _tweets


progress_bar(20, etl_complete, description="Cleaning, transforming tweets")
#Clean our initial tweets data(data from the query2_ref.txt file )
_tweets_data_df_cleaned = transform_and_clean_tweets_data(_tweets_data_unclean_df)


#Get original tweets from retweeted tweets as mentioned in the TOR (If a tweet is a retweet, the original tweet object is stored in retweeted_status)
_original_tweets_from_retweeted_tweets =  _tweets_data_df_cleaned[_tweets_data_df_cleaned['type']=='retweet']['retweeted_status']

#get the original tweets in a list and then convert it to a dataframe
_original_tweets_from_unclean_retweeted_tweets_df = pd.DataFrame(_original_tweets_from_retweeted_tweets.tolist())
_original_tweets_from_unclean_retweeted_tweets_df['retweeted_status'] = _original_tweets_from_unclean_retweeted_tweets_df.apply(lambda _: '', axis=1)

#clean the original tweets from retweeted tweets
_original_tweets_from_cleaned_retweeted_tweets_df = transform_and_clean_tweets_data(_original_tweets_from_unclean_retweeted_tweets_df)

progress_bar(30, etl_complete, description="Retrieve and clean original tweets from retweeted tweets")
#join the two tweets data (from files and original tweets of retweeted tweets)
_all_clean_tweets_data_df = pd.concat([_tweets_data_df_cleaned ,_original_tweets_from_cleaned_retweeted_tweets_df])

_all_clean_tweets_data_df = sort_tweeter_data_df(_all_clean_tweets_data_df)

#>>>>>>>>>>>>>>>>>Drop duplicates<<<<<<<<<<<<<<<<<<,

progress_bar(35, etl_complete, description="Removing duplicated tweets data from all the tweets data")
#CLEAN TWEET Data, Remove duplicates
_all_clean_tweets_data_df['tweet_id'] = _all_clean_tweets_data_df.apply(lambda row: assign_id(row.id, row.id_str), axis=1)
_all_clean_tweets_data_df.drop_duplicates(subset='tweet_id', inplace=True)
#_all_clean_tweets_data_df['user_id'] = _all_clean_tweets_data_df.user.apply(lambda row: assign_id(row['id'], row['id_str']))
_all_clean_tweets_data_df['hashtags'] =  _all_clean_tweets_data_df.entities.apply(lambda x: get_hashtags(x['hashtags']))
_all_clean_tweets_data_df = add_contacted_user(_all_clean_tweets_data_df)

def prepare_tweets_data_for_db(tweets_data):
    # PREPARE TWEETS TABLE
    # `tweet_id` BIGINT NOT NULL,`text` LONGTEXT NULL DEFAULT NULL,`created_at` TIMESTAMP NULL DEFAULT NULL,`user_id` BIGINT NULL DEFAULT NULL, 
    # Add user_id on the tweet table based on the user object
    return tweets_data.loc[:,('tweet_id','user_id','text','type','hashtags','created_at', 'contact_user')]



tweets_data_df_for_db = prepare_tweets_data_for_db(_all_clean_tweets_data_df)
progress_bar(40, etl_complete, description="Prepared tweets data to be loaded to the database")

# >>> >>> PREPARE USERS TABLE
#Extract users data
def prepare_user_data_for_db(tweets_data):
    _users_data_txt = tweets_data['user']
    _users_data_df = pd.DataFrame(_users_data_txt.tolist())

    # Based on the Database design that we have conducted we decided to extract only:
    # ['user_id', 'screen_name', 'description', 'created_at']
    #`user_id` BIGINT NOT NULL, `screen_name` TEXT NULL DEFAULT NULL,`description` TEXT NULL DEFAULT NULL, `created_at` TIMESTAMP NULL DEFAULT NULL, 
    _users_data_df['created_at'] = pd.to_datetime(_users_data_df['created_at'])
    _users_data_df['user_id'] = _users_data_df.apply(lambda row: assign_id(row.id, row.id_str), axis=1)
    _users_data_table_df =_users_data_df.loc[:, ('user_id','screen_name', 'description', 'created_at')]  
    #remove duplicates from users
    _users_data_table_df.drop_duplicates(subset='user_id', inplace=True)
    return  _users_data_table_df

progress_bar(45, etl_complete, description="Preparing users data that will be loaded to our database")
users_data_df_for_db = prepare_user_data_for_db(_all_clean_tweets_data_df)


def set_user_a_as_contact_of_user_b(user_id,_contact_tweet):
    if _contact_tweet['user_id'] != user_id:
       _contact_tweet['contact_user'] = _contact_tweet['user_id']
       _contact_tweet['user_id'] = user_id
    return _contact_tweet

def prepare_contact_tweets_table(tweets, users):
    contact_tweets=pd.DataFrame()
    for user in users.itertuples():
        contacts_tweets_a = tweets[tweets['user_id']==user.user_id]
        contacts_tweets_b = tweets[tweets['contact_user']==user.user_id]
        this_user_contacts_df = pd.concat([contacts_tweets_a, contacts_tweets_b])
        this_user_contacts_df = this_user_contacts_df.apply(lambda x:set_user_a_as_contact_of_user_b(user.user_id, x), axis=1)
        contact_tweets = pd.concat([contact_tweets, this_user_contacts_df ],ignore_index=True)

    contact_tweets=contact_tweets[contact_tweets['contact_user']>0]    
    return contact_tweets
#Generating contact tweets ...
progress_bar(50, etl_complete, description="Preparing the contact tweets data that will be loaded to db")
contact_tweets_data_for_db = prepare_contact_tweets_table(tweets_data_df_for_db, users_data_df_for_db).loc[:,('user_id', 'contact_user', 'tweet_id')]

#Loading data to users table ...

try:
    progress_bar(70, etl_complete, description="Loading users data in our MySQL database, this can take sometime, but not too long")
    users_data_df_for_db.to_sql('users', engine, index=False, if_exists='append', chunksize=5000)
   
except Exception as ex:
    print(ex)
finally:
    db_connection.close()

#Loading data to tweets table table ..."

try:
    tweets_data_df_for_db = tweets_data_df_for_db.loc[:,('tweet_id','user_id','text','type','hashtags','created_at')]
    tweets_data_df_for_db.to_sql('tweets', engine, index=False, if_exists='append', chunksize=5000)
    #progress_bar(80, etl_complete, description="Tweets data loaded to the database")
    progress_bar(80, etl_complete, description="Loading Tweets data in our MySQL database, this can take sometime, but not too long.....")
except Exception as ex:
    print(ex)
finally:
    db_connection.close()

#Loading data to contact tweets table table ...
try:
    progress_bar(90, etl_complete, description="Loading contact Tweets data in our MySQL database, this can take sometime, but not too long.......")
    contact_tweets_data_for_db.to_sql('contact_tweets', engine, index=True, index_label='contact_tweets_id', if_exists='append', chunksize=5000)
    progress_bar(100, etl_complete, description="The ETL Process is done, We have Extracted, Transformed and Loaded data to out MySQL DB. Please move on with the next step.")
except Exception as ex:
    print(ex)
finally:
    db_connection.close()






















