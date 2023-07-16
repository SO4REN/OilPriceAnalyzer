import os
import requests
from http.server import BaseHTTPRequestHandler

class Server(BaseHTTPRequestHandler):
    __files = [elem for elem in sorted(os.listdir("data")) if elem.endswith(".csv")]
    __routes = {
        "/prezzo_alle_8.csv": b""
    }

    def do_HEAD(self):
        return

    def do_POST(self):
        return

    def do_GET(self):
        self.respond({"status": 200, "content_type": "text/csv"})
        
    def handle_http(self, status, content_type):
        self.send_response(status)
        self.send_header('Content-type', content_type)
        self.end_headers()
        
        if self.path in self.__routes:
            __count = int(open(os.path.join("data", "count.txt"), "r").read())
            if __count < len(self.__files):
                self.__routes[self.path] = os.path.join("data", self.__files[__count])
                __count += 1
                with open(os.path.join("data", "count.txt"), "w") as f:
                    f.write(str(__count))
                
                data = open(self.__routes[self.path], "rb").read()
            else:
                data = requests.get("https://www.mimit.gov.it/images/exportCSV/prezzo_alle_8.csv").content

            return data
        return b"404 Not Found"

    def respond(self, opts):
        response = self.handle_http(opts["status"], opts["content_type"])
        self.wfile.write(response)
