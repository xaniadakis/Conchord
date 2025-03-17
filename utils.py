import hashlib
import re

def hash_key(key):
    key = key.lower().strip()
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2 ** 64)

def log(prefix, output):
    print(f"{prefix}{output}")


def custom_split(request):

    request = request.strip()

    # handle json array
    if "[" in request and "]" in request:
        try:
            command, json_part = request.split(" ", 1)
            return [command, json_part]
        except ValueError:
            return request.split()

    # split by spaces but preserve double-quoted text intact
    parts = re.split(r'(".*?"|\S+)', request)
    result = [part.strip() for part in parts if part.strip()]
    return result
