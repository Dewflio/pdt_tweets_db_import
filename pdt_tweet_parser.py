import time
from nbformat import write
import psycopg2, psycopg2.extras
import json, gzip, csv
from pdt_hashtable import HashTable

BLOCKSIZE = 10000

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


                if authors_inserted_count_tmp >= BLOCKSIZE:
                    authors_inserted_count_tmp = 0
                    insert_authors(conn, cursor, BLOCKSIZE, insert_vals)
                    a,b,c = get_time(block_start)
                    time_arr.append((a,b,c))
                    print(time_arr[-1])
                    insert_vals = []
                    block_start = time.time()
            else:
                continue

        if insert_vals != []:
            insert_authors(conn, cursor, BLOCKSIZE, insert_vals)
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

def insert_context_annotations(conn, cursor, page_size, insert_vals):
    psycopg2.extras.execute_values(cursor, """
            INSERT INTO context_annotations(conversation_id, context_domain_id, context_entity_id) VALUES %s;
        """, ((
            anno[0],
            anno[1],
            anno[2],
        ) for anno in insert_vals), page_size=page_size)
    conn.commit() 

def insert_context_domains(conn, cursor, page_size, insert_vals):
    psycopg2.extras.execute_values(cursor, """
            INSERT INTO context_domains(id, name, description) VALUES %s;
        """, ((
            anno[0],
            anno[1],
            anno[2],
        ) for anno in insert_vals), page_size=page_size)
    conn.commit()

def insert_context_entities(conn, cursor, page_size, insert_vals):
    psycopg2.extras.execute_values(cursor, """
            INSERT INTO context_entities(id, name, description) VALUES %s;
        """, ((
            anno[0],
            anno[1],
            anno[2],
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
            INSERT INTO hashtags(id,tag) VALUES %s;
        """, ((
            hasht[0],
            hasht[1],
        ) for hasht in insert_vals), page_size=page_size)
    conn.commit() 

def insert_conv_hashtags(conn, cursor, page_size, insert_vals):
    psycopg2.extras.execute_values(cursor, """
            INSERT INTO conversation_hashtags(conversation_id, hashtag_id) VALUES %s;
        """, ((
            hasht[0],
            hasht[1],
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

    conv_hash_inserted_count = 0
    conv_hash_inserted_count_tmp = 0

    cont_anno_inserted_count = 0
    cont_anno_inserted_count_tmp = 0
    cont_domain_inserted_count = 0
    cont_domain_inserted_count_tmp = 0
    cont_entity_inserted_count = 0
    cont_entity_inserted_count_tmp = 0

    conv_insert_vals = []
    anno_insert_vals = []
    link_insert_vals = []
    hash_insert_vals = []
    conv_hash_insert_vals = []
    cont_anno_insert_vals = []
    cont_domain_insert_vals = []
    cont_entity_insert_vals = []

    conv_time_arr = []
    conv_block_start = time.time()
    #link_time_arr = []
    #link_block_start = time.time()
    #hash_time_arr = []
    #hash_block_start = time.time()
    #link_time_arr = []
    #link_block_start = time.time()
    #anno_time_arr = []
    #anno_block_start = time.time()

    time_reset_counter = 0
    with gzip.open("D:/PDT_zadanie_1/conversations.jsonl.gz", 'r') as f:
        for line in f:
            #sanitizes the json record ----- maybe 
            line = line.decode('utf-8', errors='replace').replace('\x00', '\uFFFD')
            data = json.loads(line)
            #checks for duplicity
            if conversations_hashtable.get_val(data["id"]) == None:
                conversations_hashtable.set_val(data["id"], data["id"])

                time_reset_counter += 1

                conv_inserted_count += 1
                conv_inserted_count_tmp += 1
                #appends a conversation entry to conv_inputvals
                conv_insert_vals.append((data["id"], 
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
                #checks for entities inside the conversation
                if "entities" in data:
                    #checks for annotations and iterates through them if there are multiple
                    if "annotations" in data["entities"]:
                        for anno in data["entities"]["annotations"]:
                            anno_inserted_count += 1
                            anno_inserted_count_tmp += 1
                            anno_tuple = (data["id"], anno["normalized_text"].replace('\x00', '\uFFFD'), anno["type"], anno["probability"])
                            anno_insert_vals.append(anno_tuple)
                    #checks for links and iterates through them if there are multiple
                    if "urls" in data["entities"]:
                        for link in data["entities"]["urls"]:
                            if len(link["url"]) > 2048:
                                continue
                            link_inserted_count += 1
                            link_inserted_count_tmp += 1
                            link_title = ''
                            link_desc = ''

                            if "title" in link:
                                link_title = link["title"].replace('\x00', '\uFFFD')
                            if "description" in link:
                                link_desc = link["description"].replace('\x00', '\uFFFD')

                            link_tuple = (data["id"], link["url"], link_title, link_desc)
                            link_insert_vals.append(link_tuple)
                    
                    #checks for hashtags and iterates through them if there are multiple
                    if "hashtags" in data["entities"]:
                        unq_h = []
                        for hasht in data["entities"]["hashtags"]:
                            #we use the hash of the tag as id
                            hashed_tag = hash(hasht["tag"])
                            if hashtags_hashtable.get_val(hasht["tag"]) == None:
                                hashtags_hashtable.set_val(hasht["tag"], hashed_tag)
                                hash_inserted_count += 1
                                hash_inserted_count_tmp += 1
                                hash_tuple = (hashed_tag, hasht["tag"])
                                hash_insert_vals.append(hash_tuple)
                            if hashed_tag not in unq_h:
                                conv_hash_inserted_count += 1
                                conv_hash_inserted_count_tmp += 1
                                conv_hash_tuple = (data["id"], hashed_tag)
                                conv_hash_insert_vals.append(conv_hash_tuple)
                                unq_h.append(hashed_tag)

                    #parses context_annotations
                    if "context_annotations" in data:
                         for context_annotation in data["context_annotations"]:
                            if "domain" in context_annotation:
                                d = context_annotation["domain"]
                                if domains_hashtable.get_val(d["id"]) == None:
                                    domains_hashtable.set_val(d["id"], d["id"])
                                    desc_val = ""
                                    if "description" in d:
                                        desc_val = d["description"].replace('\x00', '\uFFFD')
                                    cont_domain_tuple = (d["id"], d["name"].replace('\x00', '\uFFFD'), desc_val)
                                    cont_domain_insert_vals.append(cont_domain_tuple)
                                    cont_domain_inserted_count += 1
                                    cont_domain_inserted_count_tmp += 1

                            if "entity" in context_annotation:
                                e = context_annotation["entity"]
                                if entities_hashtable.get_val(e["id"]) == None:
                                    entities_hashtable.set_val(e["id"], e["id"])
                                    desc_val = ""
                                    if "description" in e:
                                        desc_val = e["description"].replace('\x00', '\uFFFD')
                                    cont_entity_tuple = (e["id"], e["name"].replace('\x00', '\uFFFD'), desc_val)
                                    cont_entity_insert_vals.append(cont_entity_tuple)
                                    cont_entity_inserted_count += 1
                                    cont_entity_inserted_count_tmp += 1
                            if "domain" in context_annotation and "entity" in context_annotation:
                                cont_anno_tuple = (data["id"], context_annotation["domain"]["id"], context_annotation["entity"]["id"])
                                cont_anno_insert_vals.append(cont_anno_tuple)
                                cont_anno_inserted_count +=1
                                cont_anno_inserted_count_tmp +=1

  

                #every BLOCK_SIZE number of entries in each category, the array of entries is inserted into their respective table
                #and the arrays are reset
                #the time of insertion is appended to the respective time array
                if conv_inserted_count_tmp >= BLOCKSIZE:
                    conv_inserted_count_tmp = 0
                    insert_conversations(conn=conn, cursor=cursor, page_size=BLOCKSIZE, insert_vals=conv_insert_vals)
                    a,b,c = get_time(conv_block_start)
                    conv_time_arr.append((a,b,c))
                    print(conv_time_arr[-1])
                    conv_insert_vals = []
                    conv_block_start = time.time()
                
                if hash_inserted_count_tmp >= BLOCKSIZE:
                    hash_inserted_count_tmp = 0
                    insert_hashtags(conn=conn, cursor=cursor, page_size=BLOCKSIZE, insert_vals=hash_insert_vals)
                    hash_insert_vals = []
                
                if link_inserted_count_tmp >= BLOCKSIZE:
                    link_inserted_count_tmp = 0
                    insert_links(conn=conn, cursor=cursor, page_size=BLOCKSIZE, insert_vals=link_insert_vals)
                    link_insert_vals = []
                
                if anno_inserted_count_tmp >= BLOCKSIZE:
                    anno_inserted_count_tmp = 0
                    insert_annotations(conn=conn, cursor=cursor, page_size=BLOCKSIZE, insert_vals=anno_insert_vals)
                    anno_insert_vals = []
                
                if conv_hash_inserted_count_tmp >= BLOCKSIZE:
                    conv_hash_inserted_count_tmp = 0
                    insert_conv_hashtags(conn=conn, cursor=cursor, page_size=BLOCKSIZE, insert_vals=conv_hash_insert_vals)
                    conv_hash_insert_vals = []

                if cont_anno_inserted_count_tmp >= BLOCKSIZE:
                    cont_anno_inserted_count_tmp = 0
                    insert_context_annotations(conn=conn, cursor=cursor, page_size=BLOCKSIZE, insert_vals=cont_anno_insert_vals)
                    cont_anno_insert_vals = []
                
                if cont_domain_inserted_count_tmp >= 1000:
                    cont_domain_inserted_count_tmp = 0
                    insert_context_domains(conn=conn, cursor=cursor, page_size=1000, insert_vals=cont_domain_insert_vals)
                    cont_domain_insert_vals = []

                if cont_entity_inserted_count_tmp >= 1000:
                    cont_entity_inserted_count_tmp = 0
                    insert_context_entities(conn=conn, cursor=cursor, page_size=1000, insert_vals=cont_entity_insert_vals)
                    cont_entity_insert_vals = []
                
                
                #every 500,000 conversations, the time arrays are written into their respective files, and their arrays are reset
                #this is done to lower the number of times we write into files
                #as well as to limit the maximum used memory (as the arrays that hold timestamps are reset)
                if time_reset_counter >= 500000:
                    if conv_time_arr != []:
                        time_out_file = out_dir + "timestamps-" + start_time_str + ".csv"
                        write_to_file(time_out_file, conv_time_arr)
                        conv_time_arr = []
                    time_reset_counter = 0
                    print("times written into csvs")

           
        #handle writing the last block for every table
        if conv_insert_vals != []:
            insert_conversations(conn=conn, cursor=cursor, page_size=BLOCKSIZE, insert_vals=conv_insert_vals)
            a,b,c = get_time(conv_block_start)
            conv_time_arr.append((a,b,c))
        if anno_insert_vals != []:
            insert_annotations(conn=conn, cursor=cursor, page_size=BLOCKSIZE, insert_vals=anno_insert_vals)
        if link_insert_vals != []:
            insert_conversations(conn=conn, cursor=cursor, page_size=BLOCKSIZE, insert_vals=link_insert_vals)
        if hash_insert_vals != []:
            insert_hashtags(conn=conn, cursor=cursor, page_size=BLOCKSIZE, insert_vals=hash_insert_vals)
        if conv_hash_insert_vals != []:
            insert_conv_hashtags(conn=conn, cursor=cursor, page_size=BLOCKSIZE, insert_vals=conv_hash_insert_vals)
        if cont_anno_insert_vals != []:
            insert_context_annotations(conn=conn, cursor=cursor, page_size=BLOCKSIZE, insert_vals=cont_anno_insert_vals)
        if cont_domain_insert_vals != []:
            insert_context_domains(conn=conn, cursor=cursor, page_size=BLOCKSIZE, insert_vals=cont_domain_insert_vals)
        if cont_entity_insert_vals != []:
            insert_context_entities(conn=conn, cursor=cursor, page_size=BLOCKSIZE, insert_vals=cont_entity_insert_vals)

    #handles the remaining time stamp arrays
    if conv_time_arr != []:
        time_out_file = out_dir + "timestamps-" + start_time_str + ".csv"
        write_to_file(time_out_file, conv_time_arr)

            
    print("conversations parsed with count: " + str(conv_inserted_count))
    print("annotations parsed with count: " + str(anno_inserted_count))
    print("hashtags parsed with count: " + str(hash_inserted_count))
    print("conversation_hashtags parsed with count: " + str(conv_hash_inserted_count))
    print("links parsed with count: " + str(link_inserted_count))
    print("context_annotations parsed with count: " + str(cont_anno_inserted_count))
    print("context_domains parsed with count: " + str(cont_domain_inserted_count))
    print("context_entities parsed with count: " + str(cont_entity_inserted_count))

           
            

#connecting to the database
conn = psycopg2.connect(
   database="pdt_tweets", user='postgres', password='heslo123', host='127.0.0.1', port= '5433'
)
cursor = conn.cursor()

#parse_authors(conn, cursor)

parse_conversations_first(conn, cursor)


