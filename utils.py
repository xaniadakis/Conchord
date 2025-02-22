import hashlib
import re

def hash_key(key):
    key = key.lower().strip()  # Normalize
    # return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2**160)
    # return int(hashlib.sha1(f"keyspace:{key}".encode()).hexdigest(), 16) % (2**160)
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2 ** 64)

def log(prefix, output):
    print(f"{prefix}{output}")

def custom_split(request):
    """
    Splits the request on whitespace, but keeps substrings inside double quotes together.
    """
    parts = re.split(r'(".*?"|\S+)', request)  # Splits by spaces but preserves double-quoted text
    result = [part.strip() for part in parts if part.strip()]  # Remove empty strings and extra spaces
    return result