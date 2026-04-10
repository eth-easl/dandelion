import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

class BasicHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send_response(self, msg):
        response_data = msg.encode('utf-8')
        
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.send_header('Content-Length', str(len(response_data)))
        self.end_headers()
        self.wfile.write(response_data)

    # expects GET requests on path /get
    def do_GET(self):
        if self.path == '/get':
            self._send_response("ok get")
        elif self.path == '/get_large':
            message = 'a' * 8192
            self._send_response(message)
        else:
            self.send_response(404)

    # expects POST requests on path /post
    def do_POST(self):
        if self.path == '/post':
            self._send_response("ok post")
        else:
            self.send_response(404)

if __name__ == '__main__':
    host = '127.0.0.1'
    port = int(sys.argv[1])
    server = HTTPServer((host, port), BasicHandler)
    print(f"Server started at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
        server.server_close()