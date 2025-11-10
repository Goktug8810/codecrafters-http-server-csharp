using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;

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
                // --- 4 byte message_size ---
                byte[] sizeBuf = await ReadExactlyAsync(stream, 4);
                int messageSize = ReadInt32BigEndian(sizeBuf, 0);

                // --- Read full request ---
                byte[] msg = await ReadExactlyAsync(stream, messageSize);

                // Kafka request header (v2)
                short apiKey = ReadInt16BigEndian(msg, 0);
                short apiVersion = ReadInt16BigEndian(msg, 2);
                int correlationId = ReadInt32BigEndian(msg, 4);

                byte[] response;

                if (apiKey == 18)
                {
                    short errorCode = (apiVersion < 0 || apiVersion > 4) ? (short)35 : (short)0;
                    response = BuildApiVersionsResponseV4(correlationId, errorCode);
                }
                else if (apiKey == 75)
                {
                    string topicName = ParseDescribeTopicPartitionsRequest(msg);
                    response = BuildDescribeTopicPartitionsResponseV0(correlationId, topicName);
                }
                else
                {
                    response = BuildApiVersionsResponseV4(correlationId, 0);
                }

                await stream.WriteAsync(response, 0, response.Length);
                await stream.FlushAsync();
                Console.WriteLine($"Sent {response.Length} bytes for correlation_id={correlationId}");
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


// ---------- Request Parser ----------
static int ReadUnsignedVarInt(byte[] b, ref int o) {
    int value = 0, shift = 0;
    while (true) {
        byte x = b[o++];
        value |= (x & 0x7F) << shift;
        if ((x & 0x80) == 0) break;
        shift += 7;
    }
    return value;
}

static string ParseDescribeTopicPartitionsRequest(byte[] msg)
{
    int offset = 0;

    // Header
    offset += 2; // api_key
    offset += 2; // api_version
    offset += 4; // correlation_id

    // client_id (INT16 + bytes)
    if (offset + 2 > msg.Length) return "";
    short clientIdLen = ReadInt16BigEndian(msg, offset); offset += 2;
    if (clientIdLen > 0) {
        if (offset + clientIdLen > msg.Length) return "";
        offset += clientIdLen;
    }

    // !!! header tagged fields (UVarInt) — BUNU OKU !!!
    if (offset >= msg.Length) return "";
    _ = ReadUnsignedVarInt(msg, ref offset); // ignore header tag buffer

    // topics (COMPACT_ARRAY => UVarInt)
    if (offset >= msg.Length) return "";
    int topicsLen = ReadUnsignedVarInt(msg, ref offset);
    if (topicsLen <= 1) return ""; // empty

    // first topic: name (COMPACT_STRING => UVarInt = N+1)
    int nameLenPlus1 = ReadUnsignedVarInt(msg, ref offset);
    int realLen = nameLenPlus1 - 1;
    if (realLen < 0 || offset + realLen > msg.Length) return "";
    string topicName = Encoding.UTF8.GetString(msg, offset, realLen);
    offset += realLen;

    // topic-level tagged fields for the REQUEST topic struct (skip)
    _ = ReadUnsignedVarInt(msg, ref offset);

    // (Request'in kalan alanlarını okumaya gerek yok)
    return topicName;
}
static string ParseDescribeTopicPartitionsRequestOLD(byte[] msg)
{
    int offset = 0;

    // --- Header ---
    offset += 2; // api_key
    offset += 2; // api_version
    offset += 4; // correlation_id

    // --- Client ID (INT16 length + bytes)
    if (offset + 2 > msg.Length) return "";
    short clientIdLen = ReadInt16BigEndian(msg, offset);
    offset += 2;
    if (offset + clientIdLen > msg.Length) return "";
    offset += clientIdLen;

    // --- Topics array (INT32 length)
    if (offset + 4 > msg.Length) return "";
    short topicsLen = ReadInt16BigEndian(msg, offset);
    offset += 2;
    if (topicsLen <= 0) return "";


    // --- Topic name (INT8 length + bytes)
    if (offset + 1 > msg.Length) return "";
    byte nameLen = msg[offset++];
    if (offset + nameLen > msg.Length)
        nameLen = (byte)(msg.Length - offset);

    string topicName = Encoding.UTF8.GetString(msg, offset, nameLen);
    offset += nameLen;
    
    // --- Partitions (INT32 length)
    if (offset + 4 > msg.Length) return topicName;
    int partitionsCount = ReadInt32BigEndian(msg, offset);
    offset += 4 + (partitionsCount * 4);
    if (offset > msg.Length) offset = msg.Length;

    return topicName;
}

// ---------- Response Builders ----------
static byte[] BuildDescribeTopicPartitionsResponseV0(int correlationId, string topicName)
{
    var bytes = new List<byte>();

    void WriteByte(byte b) => bytes.Add(b);
    void WriteInt16(short v)
    {
        var x = BitConverter.GetBytes(v);
        if (BitConverter.IsLittleEndian) Array.Reverse(x);
        bytes.AddRange(x);
    }
    void WriteInt32(int v)
    {
        var x = BitConverter.GetBytes(v);
        if (BitConverter.IsLittleEndian) Array.Reverse(x);
        bytes.AddRange(x);
    }
    void WriteUnsignedVarInt(int value)
    {
        while ((value & ~0x7F) != 0)
        {
            bytes.Add((byte)((value & 0x7F) | 0x80));
            value >>= 7;
        }
        bytes.Add((byte)value);
    }

    // ---- HEADER ----
    for (int i = 0; i < 4; i++) WriteByte(0x00); // message_size placeholder
    WriteInt32(correlationId);
    WriteByte(0x00); // TAG_BUFFER (header)

    // ---- BODY ----
    WriteInt32(0);   // throttle_time_ms
    WriteUnsignedVarInt(2); // topics array length = (1 + 1)
    
    // --- Topic entry ---
    WriteInt16(3); // ErrorCode (UNKNOWN_TOPIC_OR_PARTITION)
    byte[] nameBytes = Encoding.UTF8.GetBytes(string.IsNullOrEmpty(topicName) ? "UNKNOWN_TOPIC" : topicName);
    WriteUnsignedVarInt(nameBytes.Length + 1);  // compact string length
    bytes.AddRange(nameBytes);
    WriteUnsignedVarInt(0); // topic tagged fields
    bytes.AddRange(new byte[16]); // topic_id (UUID zeros)
    WriteByte(0x00); // is_internal
    
    WriteUnsignedVarInt(1); // partitions = null array (0 + 1)

    //WriteUnsignedVarInt(2); // compact array length = (1 partition + 1)

    // Partition[0]
    //WriteInt16(0);       // ErrorCode = 0
    //WriteInt32(0);       // PartitionIndex = 0
    //WriteInt32(0);       // LeaderId = 0
    //WriteInt32(0);       // LeaderEpoch = 0
    //WriteUnsignedVarInt(1); // Replicas = empty (0 + 1)
    //WriteUnsignedVarInt(1); // ISR = empty (0 + 1)
   // WriteUnsignedVarInt(1); // EligibleLeaderReplicas = empty (0 + 1)
    //WriteUnsignedVarInt(0); // LastKnownELR = empty
  //  WriteUnsignedVarInt(0); // OfflineReplicas = empty
//    WriteUnsignedVarInt(0); // TaggedFields = none

    WriteInt32(0);          // topic_authorized_operations
    WriteByte(0xFF);        // Cursor = -1
    WriteByte(0x00);        // TagBuffer
    
    
    // ---- finalize message_size ----
    byte[] result = bytes.ToArray();
    int messageSize = result.Length - 4;
    var sizeBytes = BitConverter.GetBytes(messageSize);
    if (BitConverter.IsLittleEndian) Array.Reverse(sizeBytes);
    Array.Copy(sizeBytes, 0, result, 0, 4);
    return result;
}

static byte[] BuildApiVersionsResponseV4(int correlationId, short errorCode)
{
    var bytes = new List<byte>();

    void WriteByte(byte b) => bytes.Add(b);
    void WriteInt16(short v)
    {
        var x = BitConverter.GetBytes(v);
        if (BitConverter.IsLittleEndian) Array.Reverse(x);
        bytes.AddRange(x);
    }
    void WriteInt32(int v)
    {
        var x = BitConverter.GetBytes(v);
        if (BitConverter.IsLittleEndian) Array.Reverse(x);
        bytes.AddRange(x);
    }
    void WriteInt64(long v)
    {
        var x = BitConverter.GetBytes(v);
        if (BitConverter.IsLittleEndian) Array.Reverse(x);
        bytes.AddRange(x);
    }

    // ---- HEADER ----
    for (int i = 0; i < 4; i++) WriteByte(0x00);
    WriteInt32(correlationId);

    // ---- BODY ----
    WriteInt16(errorCode);
    WriteByte(0x03); // COMPACT_ARRAY length = 2 + 1

    // ApiVersions (key 18)
    WriteInt16(18);
    WriteInt16(0);
    WriteInt16(4);
    WriteByte(0x00);

    // DescribeTopicPartitions (key 75)
    WriteInt16(75);
    WriteInt16(0);
    WriteInt16(0);
    WriteByte(0x00);

    WriteInt32(0); // throttle_time_ms
    WriteByte(0x01); // supported_features
    WriteInt64(-1L); // finalized_features_epoch
    WriteByte(0x01); // finalized_features
    WriteByte(0x00); // zk_migration_ready
    WriteByte(0x00); // TAG_BUFFER

    // finalize
    byte[] result = bytes.ToArray();
    int messageSize = result.Length - 4;
    if (result.Length >= 4)
    {
        var sizeBytes = BitConverter.GetBytes(messageSize);
        if (BitConverter.IsLittleEndian) Array.Reverse(sizeBytes);
        Buffer.BlockCopy(sizeBytes, 0, result, 0, 4);
    }
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