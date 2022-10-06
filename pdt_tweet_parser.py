from operator import contains
import psycopg2
import json, gzip
import hashlib
from pdt_hashtable import HashTable


authors_hashtable = HashTable(1000000)



def insert_authors(conn, cursor, insert_vals):
    args_str = ', '.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in insert_vals)
    insert_query = 'insert into authors (id, name, username, description, followers_count, following_count, tweet_count, listed_count) values ' + args_str
    cursor.execute(insert_query)
    conn.commit()

def parse_authors(conn, cursor):
    authors_inserted_count = 0
    authors_inserted_count_tmp = 0
    insert_vals = []
    with gzip.open("D:/PDT_zadanie_1/authors.jsonl.gz", 'r') as f:
        for line in f:
            data = json.loads(line)#.decode("utf-8", errors="replace").replace("\x00", "\uFFFD"))
            if authors_hashtable.get_val(data["id"]) == None:
                authors_hashtable.set_val(data["id"], data["id"])

                authors_inserted_count += 1
                authors_inserted_count_tmp += 1

                if authors_inserted_count_tmp >= 10000:
                    authors_inserted_count_tmp = 0
                    insert_authors(conn, cursor, insert_vals)
                    insert_vals = []
                else:
                    insert_vals.append((data["id"], 
                    data["name"].replace('\x00', '\uFFFD'),
                     data["username"].replace('\x00', '\uFFFD'),
                      data["description"].replace('\x00', '\uFFFD'),
                       data["public_metrics"]["followers_count"],
                        data["public_metrics"]["following_count"],
                         data["public_metrics"]["tweet_count"],
                          data["public_metrics"]["listed_count"]))
 
            else:
                continue

        if insert_vals != []:
            insert_authors(conn, cursor, insert_vals)
           
            

conn = psycopg2.connect(
   database="pdt_tweets", user='postgres', password='heslo123', host='127.0.0.1', port= '5433'
)
cursor = conn.cursor()

parse_authors(conn, cursor)