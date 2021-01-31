import threading
import gzip
import time
import socket
import sys, os

host = socket.gethostname()
port = 37600

path1 = "/bigdata/mmalensek/nam/3hr_sample"

def send_file(root, file, conn):
    with gzip.open(os.path.abspath(os.path.join(root, file)), 'rb') as f:
        next(f)
        for line in f:
            # time.sleep(0.1)
            # print(line)
            conn.send(line)
        print("\n\nSent last line.\n\n")
        conn.close()

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    print("Trying to create server")
    s.bind((host, port))
    s.listen(5)
    print("Listening")

    for root,d_names,f_names in os.walk(path1):
        if len(f_names) > 0:

            for f in f_names:
                
                conn, addr = s.accept()
                print('Connected by', addr)
                print("f",f)
                
                x = threading.Thread(target=send_file, args=(root, f, conn))
                # A new thread, apart from the main thread, starts
                x.start()
                print("Active thread count",threading.activeCount())
           