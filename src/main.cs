using System;
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
        // ---- Kafka Request Header (12 byte) ----
        // 0..3  → message_size
        // 4..5  → api_key
        // 6..7  → api_version
        // 8..11 → correlation_id
        byte[] requestHeader = ReadExactly(client, 12);

        // Request’ten correlation_id çek (response tarafında da birebir aynısı dönecek)
        int correlationId = ReadInt32BigEndian(requestHeader, 8);

        // İstenen API version’u al (api_version)
        short apiVersion = ReadInt16BigEndian(requestHeader, 6);
        Console.WriteLine($"Received correlation_id={correlationId}, api_version={apiVersion}");

        // ---- Error kontrolü ----
        // Kafka 0–4 arası versiyonları destekliyor.
        // Eğer client daha yüksek bir versiyon isterse, 35 (UNSUPPORTED_VERSION) döneriz.
        short errorCode = (apiVersion < 0 || apiVersion > 4) ? (short)35 : (short)0;

        // ---- Response Fields ----
        short apiKey = 18;            // ApiVersions
        short minVersion = 0;
        short maxVersion = 4;
        int throttleTimeMs = 0;

        // ----  Buffer Hazırlığı ----
        // Flexible schema: compact array + tag buffer içerir.
        byte[] response = new byte[64];
        int offset = 0;

        // ----  message_size (4 byte placeholder) ----
        // Şimdilik boş bırakıyoruz, en sonda gerçek değeri yazacağız.
        offset += 4;

        // ----  correlation_id (INT32) ----
        WriteInt32BigEndian(response, offset, correlationId);
        offset += 4;

        // ----  error_code (INT16) ----
        WriteInt16BigEndian(response, offset, errorCode);
        offset += 2;

        // ----  compact array (api_keys) ----
        // Flexible schema: UNSIGNED_VARINT (length + 1)
        // 1 entry → (1 + 1) = 2 → 0x02
        WriteUnsignedVarInt(response, ref offset, 2);

        // ---- 9️⃣ api_key entry ----
        WriteInt16BigEndian(response, offset, apiKey); offset += 2;
        WriteInt16BigEndian(response, offset, minVersion); offset += 2;
        WriteInt16BigEndian(response, offset, maxVersion); offset += 2;

        // Her entry’nin sonunda küçük bir TAG_BUFFER alanı (boş → 0x00)
        response[offset++] = 0x00;

        // ---- 🔟 throttle_time_ms (INT32) ----
        WriteInt32BigEndian(response, offset, throttleTimeMs);
        offset += 4;

        // ---- 11️⃣ body tag buffer (boş → 0x00) ----
        response[offset++] = 0x00;

        // ---- 12️⃣ message_size’ı hesapla ----
        // message_size = header (4 hariç) + body
        int messageSize = offset - 4;
        WriteInt32BigEndian(response, 0, messageSize);

        // ---- 13️⃣ Gönder ----
        client.Send(response, offset, SocketFlags.None);

        Console.WriteLine($"Sent {offset} bytes:");
        Console.WriteLine(BitConverter.ToString(response, 0, offset));
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

//
// ---------- Helper Fonksiyonlar ----------
//

// TCP stream'den tam uzunlukta veri okur (partial read koruması)
static byte[] ReadExactly(Socket socket, int length)
{
    byte[] buffer = new byte[length];
    int offset = 0;
    while (offset < length)
    {
        int read = socket.Receive(buffer, offset, length - offset, SocketFlags.None);
        if (read == 0)
            throw new Exception("Connection closed early.");
        offset += read;
    }
    return buffer;
}

// Big-endian INT16 okur
static short ReadInt16BigEndian(byte[] buffer, int offset)
{
    byte[] temp = new byte[2];
    Array.Copy(buffer, offset, temp, 0, 2);
    if (BitConverter.IsLittleEndian) Array.Reverse(temp);
    return BitConverter.ToInt16(temp, 0);
}

// Big-endian INT32 okur
static int ReadInt32BigEndian(byte[] buffer, int offset)
{
    byte[] temp = new byte[4];
    Array.Copy(buffer, offset, temp, 0, 4);
    if (BitConverter.IsLittleEndian) Array.Reverse(temp);
    return BitConverter.ToInt32(temp, 0);
}

// Big-endian INT16 yazar
static void WriteInt16BigEndian(byte[] buffer, int offset, short value)
{
    var bytes = BitConverter.GetBytes(value);
    if (BitConverter.IsLittleEndian) Array.Reverse(bytes);
    Array.Copy(bytes, 0, buffer, offset, 2);
}

// Big-endian INT32 yazar
static void WriteInt32BigEndian(byte[] buffer, int offset, int value)
{
    var bytes = BitConverter.GetBytes(value);
    if (BitConverter.IsLittleEndian) Array.Reverse(bytes);
    Array.Copy(bytes, 0, buffer, offset, 4);
}

// UNSIGNED_VARINT (compact types için) yazar
// Kafka flexible formatında compact array/string uzunlukları varint ile kodlanır.
// Küçük sayılar tek byte olur, büyükler 2–3 byte’a kadar çıkar.
// Örn: 2 → 00000010 → [0x02]
static void WriteUnsignedVarInt(byte[] buffer, ref int offset, uint value)
{
    while (true)
    {
        byte b = (byte)(value & 0x7F); // alt 7 bit
        value >>= 7;
        if (value == 0)
        {
            buffer[offset++] = b;
            break;
        }
        else
        {
            buffer[offset++] = (byte)(b | 0x80); // devam biti (MSB = 1)
        }
    }
}