#this class was taken from this page:
#https://www.geeksforgeeks.org/hash-map-in-python/
#I only made some minor changes - changed the return value of get_val from a string to None when no record is found

class HashTable:
  
    # Create empty bucket list of given size
    def __init__(self, size):
        self.size = size
        self.hash_table = self.create_buckets()
  
    def create_buckets(self):
        return [[] for _ in range(self.size)]
  
    # Insert values into hash map
    def set_val(self, key, val):
        
        hashed_key = hash(key) % self.size
        
        bucket = self.hash_table[hashed_key]
  
        found_key = False
        for index, record in enumerate(bucket):
            record_key, record_val = record
              
            if record_key == key:
                found_key = True
                break

        if found_key:
            bucket[index] = (key, val)
        else:
            bucket.append((key, val))
  
    def get_val(self, key):

        hashed_key = hash(key) % self.size

        bucket = self.hash_table[hashed_key]
  
        found_key = False
        for index, record in enumerate(bucket):
            record_key, record_val = record

            if record_key == key:
                found_key = True
                break
  
        if found_key:
            return record_val
        else:
            return None
  
    # Remove a value with specific key
    def delete_val(self, key):
        
        hashed_key = hash(key) % self.size

        bucket = self.hash_table[hashed_key]
  
        found_key = False
        for index, record in enumerate(bucket):
            record_key, record_val = record
              
            if record_key == key:
                found_key = True
                break
        if found_key:
            bucket.pop(index)
        return
  
    # To print the items of hash map
    def __str__(self):
        return "".join(str(item) for item in self.hash_table)
