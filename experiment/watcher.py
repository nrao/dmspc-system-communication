import os 
import time 

maker_path = "some_file.done"

while not os.path.exists(maker_path) : 
    time.sleep(1)

print("COMPLETE")
