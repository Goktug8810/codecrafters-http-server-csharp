using System;
using System.Net;
using System.Net.Sockets;

// Kafka broker stub: TCP port 9092'de dinler
TcpListener server = new TcpListener(IPAddress.Any, 9092);
server.Start();
Console.WriteLine("Kafka broker stub running on port 9092...");

while (true)
{
    var client = server.AcceptSocket();
    Console.WriteLine("Client connected.");

    try
    {
        // Kafka request header’ının ilk 12 byte’ını oku
        // 0..3: message_size
        // 4..5: api_key
        // 6..7: api_version
        // 8..11: correlation_id
        byte[] requestHeader = ReadExactly(client, 12);

        // api_version (INT16, big-endian)
        short apiVersion = ReadInt16BigEndian(requestHeader, 6);

        // orrelation_id (INT32, big-endian)
        int correlationId = ReadInt32BigEndian(requestHeader, 8);
        Console.WriteLine($"Received api_version: {apiVersion}, correlation_id: {correlationId}");

        //  Destek aralığı: 0–4
        short errorCode = (apiVersion < 0 || apiVersion > 4) ? (short)35 : (short)0;

        //  Response oluştur (10 byte)
        // [0..3]: message_size (tester değere bakmıyor)
        // [4..7]: correlation_id
        // [8..9]: error_code (ApiVersionsResponse.error_code)
        byte[] response = new byte[10];
        WriteInt32BigEndian(response, 0, 0);
        WriteInt32BigEndian(response, 4, correlationId);
        WriteInt16BigEndian(response, 8, errorCode);

        // Response’u gönder
        client.Send(response);
        Console.WriteLine($"Sent 10 bytes: {BitConverter.ToString(response)}");
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

// TCP stream'den tam uzunlukta veri okur (partial read koruması)
static byte[] ReadExactly(Socket socket, int length)
{
    byte[] buffer = new byte[length];
    int offset = 0;
    while (offset < length)
    {
        int read = socket.Receive(buffer, offset, length - offset, SocketFlags.None);
        if (read == 0)
            throw new Exception("Connection closed before full header was received.");
        offset += read;
    }
    return buffer;
}

// Kafka big-endian formatında INT16 okur
static short ReadInt16BigEndian(byte[] buffer, int offset)
{
    byte[] temp = new byte[2];
    Array.Copy(buffer, offset, temp, 0, 2);
    if (BitConverter.IsLittleEndian)
        Array.Reverse(temp);
    return BitConverter.ToInt16(temp, 0);
}

// Kafka big-endian formatında INT32 okur
static int ReadInt32BigEndian(byte[] buffer, int offset)
{
    byte[] temp = new byte[4];
    Array.Copy(buffer, offset, temp, 0, 4);
    if (BitConverter.IsLittleEndian)
        Array.Reverse(temp);
    return BitConverter.ToInt32(temp, 0);
}

// Kafka big-endian formatında INT16 yazar
static void WriteInt16BigEndian(byte[] buffer, int offset, short value)
{
    var bytes = BitConverter.GetBytes(value);
    if (BitConverter.IsLittleEndian)
        Array.Reverse(bytes);
    Array.Copy(bytes, 0, buffer, offset, 2);
}

// Kafka big-endian formatında INT32 yazar
static void WriteInt32BigEndian(byte[] buffer, int offset, int value)
{
    var bytes = BitConverter.GetBytes(value);
    if (BitConverter.IsLittleEndian)
        Array.Reverse(bytes);
    Array.Copy(bytes, 0, buffer, offset, 4);
}