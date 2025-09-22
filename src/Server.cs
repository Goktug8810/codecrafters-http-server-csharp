using System.Net;
using System.Net.Sockets;
using System.Text;

using System.Net;
using System.Net.Sockets;
using System.Text;

TcpListener server = new TcpListener(IPAddress.Any, 4221);
server.Start();
Socket connection = server.AcceptSocket();

byte[] buffer = new byte[1024];
int received = connection.Receive(buffer);
string request = Encoding.ASCII.GetString(buffer, 0, received);

int endOfRequestLine = request.IndexOf("\r\n");
int endOfHeaders = request.IndexOf("\r\n\r\n");
string requestLine = request[..endOfRequestLine];
string[] parts = requestLine.Split(' ');

string path = parts[1];

string headersSection = "";
if (endOfHeaders > endOfRequestLine + 2)
{
    headersSection = request.Substring(
        endOfRequestLine + 2,
        endOfHeaders - (endOfRequestLine + 2));
}

string? userAgent = null;
if (!string.IsNullOrEmpty(headersSection))
{
    foreach (var line in headersSection.Split("\r\n"))
    {
        int colon = line.IndexOf(':');
        if (colon <= 0) continue;

        string name = line.Substring(0, colon).Trim();
        string value = line.Substring(colon + 1).Trim();

        if (name.Equals("User-Agent", StringComparison.OrdinalIgnoreCase))
        {
            userAgent = value;
            break;
        }
    }
}

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
else if (path == "/user-agent")
{
    string body = userAgent ?? "";
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



