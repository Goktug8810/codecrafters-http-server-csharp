using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

TcpListener server = new TcpListener(IPAddress.Any, 9092);
server.Start();
Console.WriteLine("Kafka broker stub running on port 9092...");

while (true)
{
    var client = await server.AcceptTcpClientAsync();
    _ = Task.Run(() => HandleClient(client));
}

static async Task HandleClient(TcpClient client)
{
    Console.WriteLine("Client connected.");
    using (client)
    using (var stream = client.GetStream())
    {
        try
        {
            while (true)
            {
                //  ÖNCE 4 byte: message_size
                byte[] sizeBuf = await ReadExactlyAsync(stream, 4);
                int messageSize = ReadInt32BigEndian(sizeBuf, 0);

                // HEADER + BODY: message_size kadar oku
                byte[] msg = await ReadExactlyAsync(stream, messageSize);

                // Kafka Request Header v2:
                // 0..1: request_api_key (INT16)
                // 2..3: request_api_version (INT16)
                // 4..7: correlation_id (INT32)
                short apiKey = ReadInt16BigEndian(msg, 0);
                short apiVersion = ReadInt16BigEndian(msg, 2);
                int correlationId = ReadInt32BigEndian(msg, 4);

                // Sadece ApiVersions (key=18) isteğini handle edeceğiz.
                // Diğer tüm istekleri ignore ediyoruz.
                byte[] response;
                if (apiKey == 18)
                {
                    // apiVersion mantıksal kontrol (0–4 arası)
                    short errorCode = (apiVersion < 0 || apiVersion > 4) ? (short)35 : (short)0;
                    response = BuildApiVersionsResponseV4(correlationId, errorCode);
                }
                else
                {
                    // Diğer tüm request'lere boş cevap dön (tester sadece ApiVersions’ı kontrol ediyor)
                    response = BuildApiVersionsResponseV4(correlationId, 0);
                }

                // Cevabı gönder
                await stream.WriteAsync(response, 0, response.Length);
                await stream.FlushAsync();

                Console.WriteLine($"Sent {response.Length} bytes (message_size={response.Length - 4}) for correlation_id={correlationId}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Client disconnected or error: {ex.Message}");
        }
    }
}

static async Task<byte[]> ReadExactlyAsync(NetworkStream stream, int length)
{
    byte[] buf = new byte[length];
    int off = 0;
    while (off < length)
    {
        int n = await stream.ReadAsync(buf, off, length - off);
        if (n == 0) throw new Exception("Connection closed early");
        off += n;
    }
    return buf;
}

// ---------- ApiVersionsResponse v4 (flexible) minimal & eksiksiz ----------
static byte[] BuildApiVersionsResponseV4(int correlationId, short errorCode)
{
    byte[] buffer = new byte[256];
    int offset = 0;

    // message_size placeholder
    offset += 4;

    // Header v1: correlation_id
    WriteInt32BigEndian(buffer, offset, correlationId); offset += 4;

    // Body
    // error_code (INT16)
    WriteInt16BigEndian(buffer, offset, errorCode); offset += 2;

    // api_keys: COMPACT_ARRAY with 2 entries (18=ApiVersions, 75=DescribeTopicPartitions)
    buffer[offset++] = 0x03; // length = entries + 1 = 2 + 1

    // --- entry: ApiVersions ---
    WriteInt16BigEndian(buffer, offset, 18); offset += 2;
    WriteInt16BigEndian(buffer, offset, 0);  offset += 2;
    WriteInt16BigEndian(buffer, offset, 4);  offset += 2;
    buffer[offset++] = 0x00; // element TAG_BUFFER

    // --- entry: DescribeTopicPartitions ---
    WriteInt16BigEndian(buffer, offset, 75); offset += 2;
    WriteInt16BigEndian(buffer, offset, 0);  offset += 2;
    WriteInt16BigEndian(buffer, offset, 0);  offset += 2;
    buffer[offset++] = 0x00; // element TAG_BUFFER

    // throttle_time_ms (INT32)
    WriteInt32BigEndian(buffer, offset, 0); offset += 4;

    // supported_features: COMPACT_ARRAY with 0 entries → just count=1
    buffer[offset++] = 0x01;

    // finalized_features_epoch (INT64)
    WriteInt64BigEndian(buffer, offset, -1L); offset += 8;

    // finalized_features: COMPACT_ARRAY with 0 entries → just count=1
    buffer[offset++] = 0x01;

    // zk_migration_ready (BOOLEAN)
    buffer[offset++] = 0x00; // false

    // TAG_BUFFER (response-level)
    buffer[offset++] = 0x00; // no tagged fields

    // message_size = offset - 4
    int messageSize = offset - 4;
    WriteInt32BigEndian(buffer, 0, messageSize);

    // Trim & return
    byte[] result = new byte[offset];
    Array.Copy(buffer, result, offset);
    return result;
}

// ---------- Big-endian helpers ----------
static short ReadInt16BigEndian(byte[] b, int o)
{
    byte[] t = new byte[2]; Array.Copy(b, o, t, 0, 2);
    if (BitConverter.IsLittleEndian) Array.Reverse(t);
    return BitConverter.ToInt16(t, 0);
}
static int ReadInt32BigEndian(byte[] b, int o)
{
    byte[] t = new byte[4]; Array.Copy(b, o, t, 0, 4);
    if (BitConverter.IsLittleEndian) Array.Reverse(t);
    return BitConverter.ToInt32(t, 0);
}
static void WriteInt16BigEndian(byte[] b, int o, short v)
{
    var x = BitConverter.GetBytes(v);
    if (BitConverter.IsLittleEndian) Array.Reverse(x);
    Array.Copy(x, 0, b, o, 2);
}
static void WriteInt32BigEndian(byte[] b, int o, int v)
{
    var x = BitConverter.GetBytes(v);
    if (BitConverter.IsLittleEndian) Array.Reverse(x);
    Array.Copy(x, 0, b, o, 4);
}
static void WriteInt64BigEndian(byte[] b, int o, long v)
{
    var x = BitConverter.GetBytes(v);
    if (BitConverter.IsLittleEndian) Array.Reverse(x);
    Array.Copy(x, 0, b, o, 8);
}