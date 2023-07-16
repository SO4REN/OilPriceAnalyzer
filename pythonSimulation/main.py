import os
import time
from http.server import HTTPServer

from server import Server


if __name__ == '__main__':
    HOST_NAME = '0.0.0.0'
    PORT_NUMBER = 7777

    with open(os.path.join("data", "count.txt"), "w") as f:
        f.write("0")

    httpd = HTTPServer((HOST_NAME, PORT_NUMBER), Server)
    print(time.asctime(), 'Server UP - %s:%s' % (HOST_NAME, PORT_NUMBER))
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    
    httpd.server_close()
    print(time.asctime(), 'Server DOWN - %s:%s' % (HOST_NAME, PORT_NUMBER))
