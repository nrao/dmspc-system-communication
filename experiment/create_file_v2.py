import time
import random
import os 

file_name = "some_file"
chunk_size_mb = 10 
num_chunks = 10

with open(file_name, "wb") as file:
    for _ in range(num_chunks):
        buffer = random.randbytes(chunk_size_mb * 1024 * 1024)
        file.write(buffer)
        current = os.stat(file_name).st_size
        print(f"(GROWING ...{current} bytes)")
        time.sleep(1)

with open(file_name + ".done", "w") as f:
    pass 
    

print("file is completed..")