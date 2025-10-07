using System.Net;
using System.Net.Sockets;

TcpListener server = new TcpListener(IPAddress.Any, 9092);
server.Start();
server.AcceptSocket(); 


while (true)
{
    var client = server.AcceptSocket();
    Console.WriteLine("Client connected.");
    client.Close();
}