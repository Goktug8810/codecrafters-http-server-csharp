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
                    string propsPath = Environment.GetCommandLineArgs().Length > 1
                        ? Environment.GetCommandLineArgs()[1]
                        : "/tmp/server.properties";
                    string logPath = MetadataParser.GetMetadataLogPath(propsPath);
                    var (topicId, partitionId) = MetadataParser.ReadMetadataLog(logPath, topicName);
                    Console.WriteLine($"Topic: {topicId}, Partition: {partitionId}");
                    
                    if (topicId == Guid.Empty)
                    {
                        response = BuildDescribeTopicPartitionsResponseV0(correlationId, topicName); // hata cevabı
                    }
                    else
                    {
                        Console.WriteLine($"Topic: {topicId}, Partition: {partitionId}");
                        response = BuildDescribeTopicPartitionsResponseV0_OK(correlationId, topicName, topicId, partitionId);
                    }
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

static byte[] BuildDescribeTopicPartitionsResponseV0_OK(int correlationId, string topicName, Guid topicId, int partitionId)
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
    WriteByte(0x00); // header tagged fields = none

    // ---- BODY ----
    WriteInt32(0);   // throttle_time_ms
    WriteUnsignedVarInt(2); // topics array length = (1 + 1)

    // --- Topic entry ---
    WriteInt16(0); // ✅ ErrorCode = 0
    byte[] nameBytes = Encoding.UTF8.GetBytes(topicName);
    WriteUnsignedVarInt(nameBytes.Length + 1); // compact string
    bytes.AddRange(nameBytes);

    // UUID
    byte[] uuidBytes = KafkaProtocolUtils.GuidToBytesBigEndian(topicId);
    bytes.AddRange(uuidBytes);

    WriteByte(0x00); // is_internal = false

    // --- Partitions array ---
    WriteUnsignedVarInt(2); // (1 partition + 1)
    WriteInt16(0);          // partition_error_code = 0
    WriteInt32(partitionId);// partition_index
    WriteInt32(0);          // leader_id
    WriteInt32(0);          // leader_epoch

    // replicas = [0]
    WriteUnsignedVarInt(2); // (1 + 1)
    WriteInt32(0);

    // isr = [0]
    WriteUnsignedVarInt(2);
    WriteInt32(0);

    // eligible_leader_replicas = [0]
    WriteUnsignedVarInt(2);
    WriteInt32(0);

    // last_known_elr = [0]
    WriteUnsignedVarInt(2);
    WriteInt32(0);

    // offline_replicas = []
    WriteUnsignedVarInt(1);

    // partition tagged fields = none
    WriteUnsignedVarInt(0);

    // topic_authorized_operations
    WriteInt32(33554432);

    // topic tagged fields = none
    WriteUnsignedVarInt(0);

    // cursor = -1 (null)
    WriteByte(0xFF);

    // response tagged fields = none
    WriteUnsignedVarInt(0);

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

// ---------- CONVERT / ENCODE / DECODE HELPERS ----------
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




static class KafkaProtocolUtils
{
    
    /// <summary>
    /// Converts a .NET Guid (little-endian internal layout) into
    /// the RFC 4122 / network byte order format that Kafka expects
    /// in its wire protocol.
    /// 
    /// Background:
    /// - Guid.ToByteArray() in .NET uses a Windows-style mixed-endian layout:
    ///   the first three fields (Data1, Data2, Data3) are little-endian,
    ///   while the last eight bytes (Data4) are already big-endian.
    /// - Kafka (and Java) encode UUIDs per RFC 4122, i.e. all fields in
    ///   big-endian (network) order.
    /// 
    /// This helper swaps the first 8 bytes appropriately so the resulting
    /// 16-byte array matches Kafka’s expected wire format.
    /// </summary>
    public static byte[] GuidToBytesBigEndian(Guid guid)
    {
        byte[] le = guid.ToByteArray();
        byte[] be = new byte[16];
        be[0] = le[3]; be[1] = le[2]; be[2] = le[1]; be[3] = le[0];
        be[4] = le[5]; be[5] = le[4];
        be[6] = le[7]; be[7] = le[6];
        Array.Copy(le, 8, be, 8, 8);
        return be;
    }
    
    public static byte[] BytesBigEndianToGuid(byte[] be)
    {
        // Kafka'daki big-endian UUID'yi .NET'in little-endian GUID'ine dönüştür
        byte[] le = new byte[16];
        le[0] = be[3]; le[1] = be[2]; le[2] = be[1]; le[3] = be[0];
        le[4] = be[5]; le[5] = be[4];
        le[6] = be[7]; le[7] = be[6];
        Array.Copy(be, 8, le, 8, 8);
        return le;
    }
}


// ----------------- METADATA PARSER -------------------- //

static class MetadataParser
{
    // topic adı alır, metadata log'dan uuid ve partition_id bulur
    public static (Guid topicId, int partitionId) ReadMetadataLog(string filePath, string topicName)
    {
        if (!File.Exists(filePath))
            throw new FileNotFoundException($"Metadata log not found: {filePath}");

        byte[] fileBytes = File.ReadAllBytes(filePath);
        byte[] nameBytes = Encoding.UTF8.GetBytes(topicName);

        // Topic adını dosyada ara (string)
        int nameIndex = IndexOf(fileBytes, nameBytes);
        if (nameIndex == -1)
        {
            Console.WriteLine($"[WARN] Topic '{topicName}' not found in metadata log, returning error response.");
            return (Guid.Empty, -1);
        }

        // Adın civarındaki UUID'yi bul
        Guid topicId = FindUuidNear(fileBytes, nameIndex, topicName);

        // UUID’ye göre partition_id bul
        int partitionId = FindPartitionIdForUuid(fileBytes, topicId);

        return (topicId, partitionId);
    }

    // Dosyada byte pattern arayan küçük yardımcı
    private static int IndexOf(byte[] haystack, byte[] needle)
    {
        for (int i = 0; i <= haystack.Length - needle.Length; i++)
        {
            bool match = true;
            for (int j = 0; j < needle.Length; j++)
            {
                if (haystack[i + j] != needle[j])
                {
                    match = false;
                    break;
                }
            }
            if (match) return i;
        }
        return -1;
    }

    // 64 byte civarında 16 byte UUID ara


    private static Guid FindUuidNear(byte[] bytes, int aroundIndex, string topicName)
    {
        int nameLength = Encoding.UTF8.GetByteCount(topicName);
        int start = aroundIndex + nameLength;
        int end = Math.Min(bytes.Length - 16, start + 128); // 128 byte civarına kadar ara

        Console.WriteLine($"\n[DEBUG] Searching UUID near topic name '{topicName}' (index={aroundIndex}, range={start}–{end})");

        for (int i = start; i < end; i++)
        {
            if (LooksLikeUuid(bytes, i))
            {
                byte[] uuidBytes = new byte[16];
                Buffer.BlockCopy(bytes, i, uuidBytes, 0, 16);

                bool looksLikeTextAfter =
                    i + 16 < bytes.Length &&
                    bytes.Skip(i + 16).Take(5).All(b => b >= 0x20 && b <= 0x7E);
                if (looksLikeTextAfter)
                    continue;

                Console.WriteLine($"[DEBUG] Potential UUID found at offset {i}: {BitConverter.ToString(uuidBytes)}");

                Guid topicGuid = new Guid(KafkaProtocolUtils.BytesBigEndianToGuid(uuidBytes));
                Console.WriteLine($"[DEBUG]   → Using .NET Guid : {topicGuid}");
                Console.WriteLine($"[DEBUG]   → ToByteArray()  : {BitConverter.ToString(topicGuid.ToByteArray())}");

                return topicGuid;
            }
        }

        throw new Exception("UUID not found near topic name.");
    }
    
    // UUID pattern’ı (örneğin çok fazla 0 olmayan 16 byte)
    private static bool LooksLikeUuid(byte[] bytes, int offset)
    {
        if (offset + 16 > bytes.Length) return false;
        int zeros = 0;
        for (int i = 0; i < 16; i++)
            if (bytes[offset + i] == 0x00) zeros++;
        // tamamen sıfır olmayan, ama makul düzeyde boşluk içeren pattern
        return zeros < 14;
    }

    // Aynı UUID tekrar geçtiğinde hemen sonrasındaki 4 byte partition_id
    private static int FindPartitionIdForUuid(byte[] bytes, Guid topicId)
    {
        byte[] uuidBytes = KafkaProtocolUtils.GuidToBytesBigEndian(topicId); // Kafka log formatında (big-endian)
        string hexUuid = BitConverter.ToString(uuidBytes).Replace("-", " ");
        Console.WriteLine($"\n[DEBUG] Searching partition ID for UUID={topicId}");
        Console.WriteLine($"[DEBUG] UUID bytes (.NET ToByteArray): {hexUuid}");

        int index = IndexOf(bytes, uuidBytes);
        if (index == -1)
        {
            Console.WriteLine("[DEBUG] UUID not found again in metadata log!");
            throw new Exception("UUID not found again for partition search.");
        }

        // Scan up to 64 bytes after the UUID for a plausible partitionId (0 or 1)
        int scanStart = index + 16;
        int scanEnd = Math.Min(bytes.Length - 4, scanStart + 64);
        for (int pos = scanStart; pos <= scanEnd; pos++)
        {
            int candidate =
                (bytes[pos] << 24) |
                (bytes[pos + 1] << 16) |
                (bytes[pos + 2] << 8) |
                bytes[pos + 3];
            if (candidate == 0 || candidate == 1)
            {
                Console.WriteLine($"[DEBUG] Found plausible partitionId={candidate} at offset={pos} (bytes: {bytes[pos]:X2} {bytes[pos+1]:X2} {bytes[pos+2]:X2} {bytes[pos+3]:X2})");
                return candidate;
            }
            else
            {
                // For debug, print candidate values
                Console.WriteLine($"[DEBUG] Candidate at offset={pos}: value={candidate} (bytes: {bytes[pos]:X2} {bytes[pos+1]:X2} {bytes[pos+2]:X2} {bytes[pos+3]:X2})");
            }
        }
        Console.WriteLine("[DEBUG] Could not find a plausible partitionId (0 or 1) within 64 bytes after UUID.");
        throw new Exception($"Partition ID (0 or 1) not found after UUID {topicId} in metadata log.");
    }
    
    public static string GetMetadataLogPath(string propertiesPath)
    {
        if (!File.Exists(propertiesPath))
            return "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

        foreach (var line in File.ReadAllLines(propertiesPath))
        {
            if (line.StartsWith("log.dirs") || line.StartsWith("metadata.log.dir"))
            {
                string dir = line.Split('=')[1].Trim();
                if (!dir.EndsWith("/")) dir += "/";
                string path = Path.Combine(dir, "__cluster_metadata-0", "00000000000000000000.log");
                if (File.Exists(path))
                    return path;
            }
        }

        // fallback default (codecrafters uses /app/kraft-combined-logs)
        string fallback = "/app/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
        return File.Exists(fallback)
            ? fallback
            : "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    }
    
}


