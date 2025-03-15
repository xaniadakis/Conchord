import hashlib
import re

def hash_key(key):
    key = key.lower().strip()  # Normalize
    # return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2**160)
    # return int(hashlib.sha1(f"keyspace:{key}".encode()).hexdigest(), 16) % (2**160)
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2 ** 64)

def log(prefix, output):
    print(f"{prefix}{output}")

import re
import json


def custom_split(request):
    """
    Splits the request on whitespace, but keeps substrings inside double quotes and JSON lists together.
    """
    request = request.strip()

    # Handle JSON array (list) separately
    if "[" in request and "]" in request:
        try:
            command, json_part = request.split(" ", 1)  # Split command and JSON list
            return [command, json_part]  # Return list with correct split
        except ValueError:
            return request.split()  # Fallback split

    # Default behavior for normal cases (quotes & words)
    parts = re.split(r'(".*?"|\S+)', request)  # Splits by spaces but preserves double-quoted text
    result = [part.strip() for part in parts if part.strip()]  # Remove empty strings and extra spaces
    return result
