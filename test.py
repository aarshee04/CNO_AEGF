from datetime import datetime

f = open("/home/gubbi/python/test.txt", "a")
f.write(f"Accessed on {str(datetime.now())}\n")
f.close()