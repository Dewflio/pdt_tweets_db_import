import psycopg2

conn = psycopg2.connect(
   database="pdt_tweets", user='postgres', password='heslo123', host='127.0.0.1', port= '5433'
)
cursor = conn.cursor()


sql = "SELECT COUNT(*) FROM authors"
cursor.execute(sql)
result = cursor.fetchone()
print("authors:\t\t\t" + str(result[0]))

sql = "SELECT COUNT(*) FROM conversations"
cursor.execute(sql)
result = cursor.fetchone()
print("conversations:\t\t\t" + str(result[0]))

sql = "SELECT COUNT(*) FROM annotations"
cursor.execute(sql)
result = cursor.fetchone()
print("annotations:\t\t\t" + str(result[0]))

sql = "SELECT COUNT(*) FROM links"
cursor.execute(sql)
result = cursor.fetchone()
print("links:\t\t\t\t" + str(result[0]))

sql = "SELECT COUNT(*) FROM hashtags"
cursor.execute(sql)
result = cursor.fetchone()
print("hashtags:\t\t\t" + str(result[0]))

sql = "SELECT COUNT(*) FROM conversation_hashtags"
cursor.execute(sql)
result = cursor.fetchone()
print("conversation_hashtags:\t\t" + str(result[0]))

sql = "SELECT COUNT(*) FROM context_annotations"
cursor.execute(sql)
result = cursor.fetchone()
print("context_annotations:\t\t" + str(result[0]))

sql = "SELECT COUNT(*) FROM context_domains"
cursor.execute(sql)
result = cursor.fetchone()
print("context_domains:\t\t" + str(result[0]))

sql = "SELECT COUNT(*) FROM context_entities"
cursor.execute(sql)
result = cursor.fetchone()
print("context_entities:\t\t" + str(result[0]))

sql = "SELECT COUNT(*) FROM conversation_references"
cursor.execute(sql)
result = cursor.fetchone()
print("conversation_references:\t" + str(result[0]))

