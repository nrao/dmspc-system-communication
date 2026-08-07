import os
import time 

file_path = "some_file"
last_size = None 

while True:
    current_size = os.stat(file_path).st_size
    if last_size is None:
        last_size = current_size
    elif last_size != current_size:
        last_size = current_size
        print(f"(GROWING ...{last_size} bytes)")
    else:
        count = 0 
        while count < 5:
            current_size = os.stat(file_path).st_size
            if last_size != current_size:
                last_size = current_size
                print(f"(GROWING ...{last_size} bytes)")
                count = 0 
            else:
                count += 1 
            time.sleep(1)
        if last_size == current_size:
            print("COMPLETE")
            break

    time.sleep(1)


