import time
from nbformat import write
import psycopg2
import json, gzip, csv
from pdt_hashtable import HashTable


authors_hashtable = HashTable(1000000)
conversations_hashtable = HashTable(1000000)

start_time = time.time()
start_time_str = time.strftime("%Y-%m-%dT%H-%M-%SZ",  time.localtime(start_time))
out_dir = "out_data/"


def write_time(file, block_start):
    current_date = time.time()
    overall_time = current_date - start_time
    current_block_time = current_date - block_start

    minutes_o, seconds_o = divmod(overall_time, 60)
    minutes_b, seconds_b = divmod(current_block_time, 60)

    current_date_str = time.strftime("%Y-%m-%dT%H:%MZ",  time.localtime(current_date))
    overall_time_str = '{:02}:{:02}'.format(int(minutes_o), int(seconds_o))
    current_block_time_str = '{:02}:{:02}'.format(int(minutes_b), int(seconds_b))

    with open(file, 'a', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow((current_date_str, overall_time_str, current_block_time_str))
    


def insert_authors(conn, cursor, insert_vals, block_start):
    args_str = ', '.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in insert_vals)
    insert_query = 'insert into authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) values ' + args_str
    cursor.execute(insert_query)
    conn.commit()
    write_time(out_dir + "authors-" + start_time_str + ".csv", block_start)

def parse_authors(conn, cursor):
    authors_inserted_count = 0
    authors_inserted_count_tmp = 0
    insert_vals = []
    block_start = time.time()
    with gzip.open("D:/PDT_zadanie_1/authors.jsonl.gz", 'r') as f:
        for line in f:
            data = json.loads(line)#.decode("utf-8", errors="replace").replace("\x00", "\uFFFD"))
            if authors_hashtable.get_val(data["id"]) == None:
                authors_hashtable.set_val(data["id"], data["id"])

                authors_inserted_count += 1
                authors_inserted_count_tmp += 1
                
                insert_vals.append((data["id"], 
                    data["name"].replace('\x00', '\uFFFD'),
                     data["username"].replace('\x00', '\uFFFD'),
                      data["description"].replace('\x00', '\uFFFD'),
                       data["public_metrics"]["followers_count"],
                        data["public_metrics"]["following_count"],
                         data["public_metrics"]["tweet_count"],
                          data["public_metrics"]["listed_count"]))

                if authors_inserted_count_tmp >= 10000:
                    authors_inserted_count_tmp = 0
                    insert_authors(conn, cursor, insert_vals, block_start)
                    insert_vals = []
                    block_start = time.time()
            else:
                continue

        if insert_vals != []:
            insert_authors(conn, cursor, insert_vals, block_start)
    print("authors parsed with count: " + str(authors_inserted_count))

def insert_conversations(conn, cursor, insert_vals, block_start):
    args_str = ', '.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in insert_vals)
    insert_query = 'insert into authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) values ' + args_str
    cursor.execute(insert_query)
    conn.commit()
    write_time(out_dir + "authors-" + start_time_str + ".csv", block_start)


def parse_conversations_first(conn, cursor):
    conv_inserted_count = 0
    conv_inserted_count_tmp = 0
    conv_insert_vals = []
    block_start = time.time()
    with gzip.open("D:/PDT_zadanie_1/conversations.jsonl.gz", 'r') as f:
        for line in f:
            data = json.loads(line)
            if conversations_hashtable.get_val(data["id"]) == None:
                conversations_hashtable.set_val(data["id"], data["id"])

                conv_inserted_count += 1
                conv_inserted_count_tmp += 1
                insert_vals.append((data["id"], 
                    data["author_id"],
                     data["text"].replace('\x00', '\uFFFD'),
                      data["possibly_sensitive"],
                       data["lang"].replace('\x00', '\uFFFD'),
                        data["source"].replace('\x00', '\uFFFD'),
                         data["public_metrics"]["retweet_count"],
                          data["public_metrics"]["reply_count"],
                          data["public_metrics"]["like_count"],
                          data["public_metrics"]["quote_count"],
                          data["created_at"]))

                if conv_inserted_count_tmp >= 10000:
                    conv_inserted_count_tmp = 0
                    insert_authors(conn, cursor, insert_vals, block_start)
                    insert_vals = []
                    block_start = time.time()                    
 
            else:
                continue

        if insert_vals != []:
            insert_authors(conn, cursor, insert_vals, block_start)
           
            

conn = psycopg2.connect(
   database="pdt_tweets", user='postgres', password='heslo123', host='127.0.0.1', port= '5433'
)
cursor = conn.cursor()

parse_authors(conn, cursor)


