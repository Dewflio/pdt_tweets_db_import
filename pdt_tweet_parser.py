import time
from nbformat import write
import psycopg2, psycopg2.extras
import json, gzip, csv
from pdt_hashtable import HashTable

authors_dict = {}
conversations_dict = {}
authors_hashtable = HashTable(1000000)

conversations_hashtable = HashTable(1000000)
hashtags_hashtable = HashTable(1000000)

entities_hashtable = HashTable(1000000)
domains_hashtable = HashTable(1000000)


start_time = time.time()
start_time_str = time.strftime("%Y-%m-%dT%H-%M-%SZ",  time.localtime(start_time))
out_dir = "out_data/"

def write_to_file(filename, arr):
    with open(filename, 'a', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        for time_stamp in arr:
            writer.writerow(time_stamp)

def get_time(block_start):
    current_date = time.time()
    overall_time = current_date - start_time
    current_block_time = current_date - block_start

    minutes_o, seconds_o = divmod(overall_time, 60)
    minutes_b, seconds_b = divmod(current_block_time, 60)

    current_date_str = time.strftime("%Y-%m-%dT%H:%MZ",  time.localtime(current_date))
    overall_time_str = '{:02}:{:02}'.format(int(minutes_o), int(seconds_o))
    current_block_time_str = '{:02}:{:02}'.format(int(minutes_b), int(seconds_b))

    return current_date_str, overall_time_str, current_block_time_str
    
def insert_authors(conn, cursor, page_size, insert_vals):
    psycopg2.extras.execute_values(cursor, """
            INSERT INTO authors VALUES %s;
        """, ((
            author[0],
            author[1],
            author[2],
            author[3],
            author[4],
            author[5],
            author[6],
            
        ) for author in insert_vals), page_size=page_size)
    conn.commit()

def parse_authors(conn, cursor):
    authors_inserted_count = 0
    authors_inserted_count_tmp = 0
    insert_vals = []
    block_start = time.time()
    time_arr = []
    #TODO SANITIZE TEXT AT THE START
    with gzip.open("D:/PDT_zadanie_1/authors.jsonl.gz", 'r') as f:
        for line in f:
            data = json.loads(line)
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
                    insert_authors(conn, cursor, 10000, insert_vals)
                    a,b,c = get_time(block_start)
                    time_arr.append((a,b,c))
                    print(time_arr[-1])
                    insert_vals = []
                    block_start = time.time()
            else:
                continue

        if insert_vals != []:
            insert_authors(conn, cursor, 10000, insert_vals)
            a,b,c = get_time(block_start)
            time_arr.append((a,b,c))

    time_out_file = out_dir + "authors-" + start_time_str + ".csv"
    with open(time_out_file, 'w', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        for time_stamp in time_arr:
            writer.writerow(time_stamp)
            
    print("authors parsed with count: " + str(authors_inserted_count))

def insert_conversations(conn, cursor, page_size, insert_vals):
    psycopg2.extras.execute_values(cursor, """
            INSERT INTO conversations VALUES %s;
        """, ((
            conv[0],
            conv[1],
            conv[2],
            conv[3],
            conv[4],
            conv[5],
            conv[6],
            conv[7],
            conv[8],
            conv[9],
            conv[10],
            
        ) for conv in insert_vals), page_size=page_size)
    conn.commit()

def insert_annotations(conn, cursor, page_size, insert_vals):
    psycopg2.extras.execute_values(cursor, """
            INSERT INTO annotations(conversation_id, value, type, probability) VALUES %s;
        """, ((
            anno[0],
            anno[1],
            anno[2],
            anno[3],
        ) for anno in insert_vals), page_size=page_size)
    conn.commit() 

def insert_links(conn, cursor, page_size, insert_vals):
    psycopg2.extras.execute_values(cursor, """
            INSERT INTO links(conversation_id, url, title, description) VALUES %s;
        """, ((
            link[0],
            link[1],
            link[2],
            link[3],        
        ) for link in insert_vals), page_size=page_size)
    conn.commit() 

def insert_hashtags(conn, cursor, page_size, insert_vals):
    psycopg2.extras.execute_values(cursor, """
            INSERT INTO hashtags(tag) VALUES %s;
        """, ((
            hasht[0],
        ) for hasht in insert_vals), page_size=page_size)
    conn.commit() 

def insert_context_domains(conn, cursor, page_size, insert_vals):
    psycopg2.extras.execute_values(cursor, """
            INSERT INTO context_domains VALUES %s;
        """, ((
            item[0],
            item[1],
            item[2],
        ) for item in insert_vals), page_size=page_size)
    conn.commit()

def insert_context_entities(conn, cursor, page_size, insert_vals):
    psycopg2.extras.execute_values(cursor, """
            INSERT INTO context_entities VALUES %s;
        """, ((
            item[0],
            item[1],
            item[2],
        ) for item in insert_vals), page_size=page_size)
    conn.commit() 

def parse_conversations_first(conn, cursor):
    conv_inserted_count = 0
    conv_inserted_count_tmp = 0

    anno_inserted_count = 0
    anno_inserted_count_tmp = 0

    link_inserted_count = 0
    link_inserted_count_tmp = 0

    hash_inserted_count = 0
    hash_inserted_count_tmp = 0

    conv_insert_vals = []
    anno_insert_vals = []
    link_insert_vals = []
    hash_insert_vals = []

    link_time_arr = []
    link_block_start = time.time()
    hash_time_arr = []
    hash_block_start = time.time()
    link_time_arr = []
    link_block_start = time.time()
    conv_time_arr = []
    conv_block_start = time.time()
    anno_time_arr = []
    anno_block_start = time.time()

    time_reset_counter = 0




    with gzip.open("D:/PDT_zadanie_1/conversations.jsonl.gz", 'r') as f:
        for line in f:
            line = line.decode('utf-8', errors='replace').replace('\x00', '\uFFFD')
            data = json.loads(line)
            if conversations_hashtable.get_val(data["id"]) == None:
                conversations_hashtable.set_val(data["id"], data["id"])

                time_reset_counter += 1

                conv_inserted_count += 1
                conv_inserted_count_tmp += 1
                conv_insert_vals.append((data["id"], 
                    data["author_id"],
                     data["text"],
                      data["possibly_sensitive"],
                       data["lang"],
                        data["source"],
                         data["public_metrics"]["retweet_count"],
                          data["public_metrics"]["reply_count"],
                          data["public_metrics"]["like_count"],
                          data["public_metrics"]["quote_count"],
                          data["created_at"]))
                if "entities" in data:
                    if "annotations" in data["entities"]:
                        for anno in data["entities"]["annotations"]:
                            anno_inserted_count += 1
                            anno_inserted_count_tmp += 1
                            anno_tuple = (data["id"], anno["normalized_text"], anno["type"], anno["probability"])
                            anno_insert_vals.append(anno_tuple)
                    if "urls" in data["entities"]:
                        for link in data["entities"]["urls"]:
                            if len(link["url"]) > 2048:
                                continue
                            link_inserted_count += 1
                            link_inserted_count_tmp += 1
                            link_title = ''
                            link_desc = ''

                            if "title" in link:
                                link_title = link["title"]
                            if "description" in link:
                                link_desc = link["description"]

                            link_tuple = (data["id"], link["url"], link_title, link_desc)
                            link_insert_vals.append(link_tuple)
                    
                    if "hashtags" in data["entities"]:
                        for hasht in data["entities"]["hashtags"]:
                            if hashtags_hashtable.get_val(hasht["tag"]) == None and hasht["tag"] == "":
                                hashtags_hashtable.set_val(hasht["tag"], hasht["tag"])
                                hash_inserted_count += 1
                                hash_inserted_count_tmp += 1
                                hash_tuple = (hasht["tag"])
                                hash_insert_vals.append(hash_tuple)
  


                if conv_inserted_count_tmp >= 10000:
                    conv_inserted_count_tmp = 0
                    insert_conversations(conn=conn, cursor=cursor, page_size=10000, insert_vals=conv_insert_vals)
                    a,b,c = get_time(conv_block_start)
                    conv_time_arr.append((a,b,c))
                    print(conv_time_arr[-1])
                    conv_insert_vals = []
                    conv_block_start = time.time()
                
                if hash_inserted_count_tmp >= 10000:
                    hash_inserted_count_tmp = 0
                    insert_hashtags(conn=conn, cursor=cursor, page_size=10000, insert_vals=hash_insert_vals)
                    a,b,c = get_time(hash_block_start)
                    hash_time_arr.append((a,b,c))
                    hash_insert_vals = []
                    hash_block_start = time.time()
                
                if link_inserted_count_tmp >= 10000:
                    link_inserted_count_tmp = 0
                    insert_links(conn=conn, cursor=cursor, page_size=10000, insert_vals=link_insert_vals)
                    a,b,c = get_time(link_block_start)
                    link_time_arr.append((a,b,c))
                    link_insert_vals = []
                    link_block_start = time.time()
                
                if anno_inserted_count_tmp >= 10000:
                    anno_inserted_count_tmp = 0
                    insert_annotations(conn=conn, cursor=cursor, page_size=10000, insert_vals=anno_insert_vals)
                    a,b,c = get_time(anno_block_start)
                    anno_time_arr.append((a,b,c))
                    anno_insert_vals = []
                    anno_block_start = time.time()
                
                if time_reset_counter >= 500000:
                    if conv_time_arr != []:
                        time_out_file = out_dir + "conversations-" + start_time_str + ".csv"
                        write_to_file(time_out_file, conv_time_arr)
                        conv_time_arr = []
                    if anno_time_arr != []:
                        time_out_file = out_dir + "annotations-" + start_time_str + ".csv"
                        write_to_file(time_out_file, anno_time_arr)
                        anno_time_arr = []
                    if hash_time_arr != []:
                        time_out_file = out_dir + "hashtags-" + start_time_str + ".csv"
                        write_to_file(time_out_file, hash_time_arr)
                        hash_time_arr = []
                    if link_time_arr != []:
                        time_out_file = out_dir + "links-" + start_time_str + ".csv"
                        write_to_file(time_out_file, link_time_arr)
                        link_time_arr
                    time_reset_counter = 0
                    print("times written into csvs")

           

        if conv_insert_vals != []:
            insert_conversations(conn=conn, cursor=cursor, page_size=10000, insert_vals=conv_insert_vals)
            a,b,c = get_time(conv_block_start)
            conv_time_arr.append((a,b,c))
        if anno_insert_vals != []:
            insert_annotations(conn=conn, cursor=cursor, page_size=10000, insert_vals=anno_insert_vals)
            a,b,c = get_time(anno_block_start)
            anno_time_arr.append((a,b,c))
        if link_insert_vals != []:
            insert_conversations(conn=conn, cursor=cursor, page_size=10000, insert_vals=link_insert_vals)
            a,b,c = get_time(link_block_start)
            link_time_arr.append((a,b,c))
        if hash_insert_vals != []:
            insert_hashtags(conn=conn, cursor=cursor, page_size=10000, insert_vals=hash_insert_vals)
            a,b,c = get_time(hash_block_start)
            hash_time_arr.append((a,b,c))

    if conv_time_arr != []:
        time_out_file = out_dir + "conversations-" + start_time_str + ".csv"
        write_to_file(time_out_file, conv_time_arr)
    if anno_time_arr != []:
        time_out_file = out_dir + "annotations-" + start_time_str + ".csv"
        write_to_file(time_out_file, anno_time_arr)
    if hash_time_arr != []:
        time_out_file = out_dir + "hashtags-" + start_time_str + ".csv"
        write_to_file(time_out_file, hash_time_arr)
    if link_time_arr != []:
        time_out_file = out_dir + "links-" + start_time_str + ".csv"
        write_to_file(time_out_file, link_time_arr)

            
    print("conversations parsed with count: " + str(conv_inserted_count))
    print("annotations parsed with count: " + str(anno_inserted_count))
    print("hashtags parsed with count: " + str(hash_inserted_count))
    print("links parsed with count: " + str(link_inserted_count))

           
            

conn = psycopg2.connect(
   database="pdt_tweets", user='postgres', password='heslo123', host='127.0.0.1', port= '5433'
)
cursor = conn.cursor()

#parse_authors(conn, cursor)

parse_conversations_first(conn, cursor)


