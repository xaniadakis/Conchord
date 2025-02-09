import hashlib

def hash_key(key):
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2**160)
