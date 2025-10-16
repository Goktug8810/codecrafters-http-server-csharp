using System.Net;
using System.Net.Sockets;

TcpListener server = new TcpListener(IPAddress.Any, 9092);
server.Start();
Console.WriteLine("Kafka broker stub running on port 9092...");

while (true)
{
    var client = server.AcceptSocket();
    Console.WriteLine("Client connected.");

    try
    {
        // Kafka yanıtı için 8 byte'lık bir buffer
        // [0-3]: message_size (0)
        // [4-7]: correlation_id (7)
        byte[] buffer = new byte[8];
        WriteInt32BigEndian(buffer, 0, 3); // offset 0
        WriteInt32BigEndian(buffer, 4, 7); // offset 4
        // Byte'ları doğrudan sokete gönder
        client.Send(buffer);
        Console.WriteLine("Sent 8 bytes: " + BitConverter.ToString(buffer));
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error: {ex.Message}");
    }
    finally
    {
        client.Close();
    }
}


static void WriteInt32BigEndian(byte[] buffer, int offset, int value)
{
    var bytes = BitConverter.GetBytes(value);
    if (BitConverter.IsLittleEndian)
        Array.Reverse(bytes);
    Array.Copy(bytes, 0, buffer, offset, 4);
}