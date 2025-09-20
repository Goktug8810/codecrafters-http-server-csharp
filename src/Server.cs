using System.Net;
using System.Net.Sockets;
using System.Text;

TcpListener server = new TcpListener(IPAddress.Any, 4221);
server.Start();
Socket client = server.AcceptSocket();

string response = "HTTP/1.1 200 OK\r\n\r\n";
byte[] data = Encoding.ASCII.GetBytes(response);


client.Send(data);
