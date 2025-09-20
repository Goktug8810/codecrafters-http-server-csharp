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

string response = (path == "/")
    ? "HTTP/1.1 200 OK\r\n\r\n"
    : "HTTP/1.1 404 Not Found\r\n\r\n";


byte[] data = Encoding.ASCII.GetBytes(response);
connection.Send(data);



