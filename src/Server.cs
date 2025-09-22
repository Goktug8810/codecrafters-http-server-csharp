using System.Net;
using System.Net.Sockets;
using System.Text;

TcpListener server = new TcpListener(IPAddress.Any, 4221);
server.Start();
Socket connection = server.AcceptSocket();

byte[] buffer = new byte[1024];
int received = connection.Receive(buffer);
string request = Encoding.ASCII.GetString(buffer, 0, received);


int endOfLine = request.IndexOf("\r\n");
string requestLine = request[..endOfLine];
string[] parts = requestLine.Split(' ');


string path = parts[1];

string response;

if (path == "/")
{
    response = "HTTP/1.1 200 OK\r\n\r\n";
}
else if (path.StartsWith("/echo/"))
{
    string body = path.Substring("/echo/".Length);
    int length = Encoding.ASCII.GetByteCount(body); 
    response =
        "HTTP/1.1 200 OK\r\n" +
        "Content-Type: text/plain\r\n" +
        $"Content-Length: {length}\r\n" +
        "\r\n" +
        body;
}
else
{
    response = "HTTP/1.1 404 Not Found\r\n\r\n";
}


byte[] data = Encoding.ASCII.GetBytes(response);
connection.Send(data);



