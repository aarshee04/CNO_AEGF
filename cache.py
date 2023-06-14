from functools import lru_cache
import sys

cache = dict()

def testCache(str):
    if str not in cache:
        print("Not in cache")
        cache[str] = f"Hello {str}"
        return f"Hello {str}"
    
    return cache[str]

print(testCache(sys.argv[1]))
