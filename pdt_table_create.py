import psycopg2

conn = psycopg2.connect(
   database="pdt_tweets", user='postgres', password='heslo123', host='127.0.0.1', port= '5433'
)
cursor = conn.cursor()

sql_authors ='''CREATE TABLE IF NOT EXISTS authors(
   id int8 PRIMARY KEY,
   name varchar(255),
   username varchar(255),
   description text,
   followers_count int4,
   following_count int4,
   tweet_count int4,
   listed_count int4
)'''

#changed int8 to BIGSERIAL to autoincrement
sql_conversation_hashtags ='''CREATE TABLE IF NOT EXISTS conversation_hashtags(
   id BIGSERIAL PRIMARY KEY,
   conversation_id int8 NOT NULL,
   hashtag_id int8 NOT NULL
)'''



sql_conversations ='''CREATE TABLE IF NOT EXISTS conversations(
   id int8 PRIMARY KEY,
   author_id int8 NOT NULL,
   content text NOT NULL,
   possibly_sensitive bool NOT NULL,
   language varchar(3) NOT NULL,
   source text NOT NULL,
   retweet_count int4,
   reply_count int4,
   like_count int4,
   quota_count int4,
   created_at timestamp with time zone NOT NULL
)'''

sql_conversation_references ='''CREATE TABLE IF NOT EXISTS conversation_references(
   id int8 PRIMARY KEY,
   conversation_id int8 NOT NULL,
   parent_id int8 NOT NULL,
   type varchar(20) NOT NULL
)'''

sql_links ='''CREATE TABLE IF NOT EXISTS links(
   id BIGSERIAL PRIMARY KEY,
   conversation_id int8 NOT NULL,
   url varchar(2048) NOT NULL,
   title text,
   description text
)'''

sql_annotations ='''CREATE TABLE IF NOT EXISTS annotations(
   id BIGSERIAL PRIMARY KEY,
   conversation_id int8 NOT NULL,
   value text NOT NULL,
   type text NOT NULL,
   probability numeric(4,3) NOT NULL
)'''

#got rid of BIGSERIAL for autoincrement - changing id to be the hash of the tag :)
sql_hashtags ='''CREATE TABLE IF NOT EXISTS hashtags(
   id int8 PRIMARY KEY,
   tag text
)'''
#tmp got rid of UNIQUE in column tag 

#changed int8 to BIGSERIAL to autoincrement
sql_context_annotations ='''CREATE TABLE IF NOT EXISTS context_annotations(
   id BIGSERIAL PRIMARY KEY,
   conversation_id int8 NOT NULL,
   context_domain_id int8 NOT NULL,
   context_entity_id int8 NOT NULL
)'''

sql_context_entities ='''CREATE TABLE IF NOT EXISTS context_entities(
   id int8 PRIMARY KEY,
   name varchar(255) NOT NULL,
   description text
)'''

sql_context_domains ='''CREATE TABLE IF NOT EXISTS context_domains(
   id int8 PRIMARY KEY,
   name varchar(255) NOT NULL,
   description text
)'''

cursor.execute(sql_authors)
cursor.execute(sql_context_domains)
cursor.execute(sql_context_entities)
cursor.execute(sql_context_annotations)
cursor.execute(sql_annotations)
cursor.execute(sql_links)
cursor.execute(sql_conversation_references)
cursor.execute(sql_conversations)
cursor.execute(sql_hashtags)
cursor.execute(sql_conversation_hashtags)


conn.commit()

conn.close()

