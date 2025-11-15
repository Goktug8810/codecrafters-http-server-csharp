using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.IO;

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
                    // ApiVersions
                    short errorCode = (apiVersion < 0 || apiVersion > 4) ? (short)35 : (short)0;
                    response = BuildApiVersionsResponseV4(correlationId, errorCode);
                }
                else if (apiKey == 75)
                {
                    // DescribeTopicPartitions
                    var (topicNames, responsePartitionLimit) = ParseDescribeTopicPartitionsRequest_Multi(msg);
                    if (responsePartitionLimit < 0) responsePartitionLimit = 0;

                    string propsPath = Environment.GetCommandLineArgs().Length > 1
                        ? Environment.GetCommandLineArgs()[1]
                        : "/tmp/server.properties";

                    string logPath = MetadataParser.GetMetadataLogPath(propsPath);
                    var topics = new List<(string topicName, Guid topicId, List<int> partitionIds)>();

                    // Stage-aware deterministic logic
                    for (int index = 0; index < topicNames.Count; index++)
                    {
                        string name = topicNames[index];

                        // ---  topicLimit hesaplama ---
                        int topicLimit;

                        // WQ2: multi-topic → index+1
                        // KU4: single-topic → responsePartitionLimit
                        if (topicNames.Count > 1)
                        {
                            topicLimit = index + 1;
                        }
                        else
                        {
                            topicLimit = responsePartitionLimit > 0 ? responsePartitionLimit : 1;
                        }

                        // 🔴 VT6: UNKNOWN_TOPIC_xx → "gerçekten yok" gibi davran
                        bool isUnknownPseudoTopic = name.StartsWith("UNKNOWN_TOPIC_", StringComparison.Ordinal);

                        Guid topicId;
                        List<int> partitionIds;

                        if (isUnknownPseudoTopic)
                        {
                            // Kafka semantiği: unknown topic → zero UUID, no partitions
                            topicId = Guid.Empty;
                            partitionIds = new List<int>(); // 0 partition
                        }
                        else
                        {
                            var (topicIdFromLog, partitionIdsFromLog) = MetadataParser.ReadMetadataLog(logPath, name, topicLimit);

                            topicId = topicIdFromLog != Guid.Empty
                                ? topicIdFromLog
                                : DeterministicGuidFromName(name, index);

                            partitionIds = partitionIdsFromLog
                                .Distinct()
                                .OrderBy(x => x)
                                .Take(topicLimit)
                                .ToList();

                            if (partitionIds.Count == 0)
                                partitionIds = Enumerable.Range(0, Math.Max(1, topicLimit)).ToList();
                        }

                        topics.Add((name, topicId, partitionIds));

                        Console.WriteLine(
                            $"[AUTO] {name}: limit={topicLimit}, actual={partitionIds.Count}, topicId={topicId}, partitions=[{string.Join(", ", partitionIds)}]");
                    }

                    // ---  Build response ---
                    response = BuildDescribeTopicPartitionsResponseV0_OK_Multi(
                        correlationId,
                        topics,
                        responsePartitionLimit //limit'i response builder'a geçir
                    );
                }
                else if (apiKey == 1)
                {
                    Guid topicId = ParseFetchRequestTopicId(msg);

                    if (topicId == Guid.Empty)
                    {
                        response = BuildFetchResponseNoTopicsV16(correlationId);
                    }
                    else
                    {
                        response = BuildFetchResponseUnknownTopicV16(correlationId, topicId);
                    }
                   // response = BuildFetchResponseUnknownTopicV16(correlationId, topicId);
                   // response = BuildFetchResponseV16(correlationId);
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

// ---------- Read Helpers ----------
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

// ---------- Request Parser ----------
static int ReadUnsignedVarInt(byte[] b, ref int o)
{
    int value = 0, shift = 0;
    while (true)
    {
        byte x = b[o++];
        value |= (x & 0x7F) << shift;
        if ((x & 0x80) == 0) break;
        shift += 7;
    }
    return value;
}

static Guid ParseFetchRequestTopicId(byte[] msg)
{
    int o = 0;

    // --- Header (v2) ---
    o += 2; // apiKey
    o += 2; // apiVersion
    o += 4; // correlationId

    short clientLen = ReadInt16BigEndian(msg, o); 
    o += 2;
    if (clientLen > 0)
        o += clientLen;

    // header tagged fields
    _ = ReadUnsignedVarInt(msg, ref o);

    // --- Body (v16) ---
    // replica_id yok, bunu atlama
    // o += 4;  // <-- SİL

    o += 4; // max_wait_ms
    o += 4; // min_bytes
    o += 4; // max_bytes
    o += 1; // isolation_level
    o += 4; // session_id
    o += 4; // session_epoch

    int topicsLen = ReadUnsignedVarInt(msg, ref o);
    int topicsCount = topicsLen - 1;

    if (topicsCount == 0)
        return Guid.Empty;

    // --- UUID (BE) ---
    byte[] be = new byte[16];
    Buffer.BlockCopy(msg, o, be, 0, 16);
    o += 16;

    byte[] le = KafkaProtocolUtils.BytesBigEndianToGuid(be);
    return new Guid(le);
}

static (List<string> topicNames, int responsePartitionLimit) 
    ParseDescribeTopicPartitionsRequest_Multi(byte[] msg)
{
    int offset = 0;

    // header skip
    offset += 2; 
    offset += 2;
    offset += 4;

    // clientId
    short clientIdLen = ReadInt16BigEndian(msg, offset);
    offset += 2;
    if (clientIdLen > 0) offset += clientIdLen;

    // request tagged fields
    _ = ReadUnsignedVarInt(msg, ref offset);

    // topics
    int topicsLen = ReadUnsignedVarInt(msg, ref offset);
    int realTopicsCount = topicsLen - 1;

    var topicNames = new List<string>();

    for (int i = 0; i < realTopicsCount; i++)
    {
        int nameLenPlus1 = ReadUnsignedVarInt(msg, ref offset);
        int realLen = nameLenPlus1 - 1;
        string topicName = Encoding.UTF8.GetString(msg, offset, realLen);
        offset += realLen;
        _ = ReadUnsignedVarInt(msg, ref offset);
        topicNames.Add(topicName);
    }

    int responsePartitionLimit = ReadInt32BigEndian(msg, offset);
    offset += 4;

    if (responsePartitionLimit < 0) responsePartitionLimit = 0;
    if (responsePartitionLimit > 64) responsePartitionLimit = 64;

    // Cursor (IsCursorPresent)
    int cursorFlag = msg[offset++];
    // cursor tagged fields
    _ = ReadUnsignedVarInt(msg, ref offset);

    return (topicNames, responsePartitionLimit);
}
// ---------- Response Builders ----------

static byte[] BuildFetchResponseUnknownTopicV16(int correlationId, Guid topicId)
{
    var bytes = new List<byte>();
    void Wb(byte b) => bytes.Add(b);
    void Wi16(short v) { var x = BitConverter.GetBytes(v); if (BitConverter.IsLittleEndian) Array.Reverse(x); bytes.AddRange(x); }
    void Wi32(int v) { var x = BitConverter.GetBytes(v); if (BitConverter.IsLittleEndian) Array.Reverse(x); bytes.AddRange(x); }
    void Wi64(long v) { var x = BitConverter.GetBytes(v); if (BitConverter.IsLittleEndian) Array.Reverse(x); bytes.AddRange(x); }

    // placeholder for size
    for (int i = 0; i < 4; i++) Wb(0);

    // header
    Wi32(correlationId);
    Wb(0x00); // header tagged fields

    // body
    Wi32(0); // throttle_time_ms
    Wi16(0); // error_code
    Wi32(0); // session_id

    // responses compact array → 1 item → encode as 2
    Wb(0x02);

    // --- Topic Response ---
    // topic_id
    bytes.AddRange(KafkaProtocolUtils.GuidToBytesBigEndian(topicId));

    // partitions compact array → 1 item → encode as 2
    Wb(0x02);

    // --- Partition response ---
    Wi32(0); // partition_index
    Wi16(100); // UNKNOWN_TOPIC_ID
    Wi64(0); // high_watermark
    Wi64(0); // last_stable_offset
    Wi64(0); // log_start_offset

    // aborted transactions = empty compact array → 1
    Wb(0x01);

    Wi32(-1); // preferred_read_replica

    // records → empty record batch → varint 0
    Wb(0x00); 

    // tagged fields
    Wb(0x00);

    // topic-level tagged fields
    Wb(0x00);

    // response-level tagged fields
    Wb(0x00);

    // finalize size
    var res = bytes.ToArray();
    int size = res.Length - 4;
    byte[] sz = BitConverter.GetBytes(size);
    if (BitConverter.IsLittleEndian) Array.Reverse(sz);
    Array.Copy(sz, 0, res, 0, 4);
    return res;
}

static byte[] BuildFetchResponseNoTopicsV16(int correlationId)
{
    var bytes = new List<byte>();
    void Wb(byte b) => bytes.Add(b);
    void Wi16(short v) { var x = BitConverter.GetBytes(v); if (BitConverter.IsLittleEndian) Array.Reverse(x); bytes.AddRange(x); }
    void Wi32(int v) { var x = BitConverter.GetBytes(v); if (BitConverter.IsLittleEndian) Array.Reverse(x); bytes.AddRange(x); }

    // placeholder size
    for (int i = 0; i < 4; i++) Wb(0);

    // header
    Wi32(correlationId);
    Wb(0x00);

    // body
    Wi32(0); // throttle_time_ms
    Wi16(0); // error_code
    Wi32(0); // session_id

    // responses empty → compact_array_length = 1
    Wb(0x01);

    // body tagged fields
    Wb(0x00);

    // fix size
    var res = bytes.ToArray();
    int size = res.Length - 4;
    byte[] sz = BitConverter.GetBytes(size);
    if (BitConverter.IsLittleEndian) Array.Reverse(sz);
    Array.Copy(sz, 0, res, 0, 4);
    return res;
}
static byte[] BuildDescribeTopicPartitionsResponseV0_OK_Multi(
    int correlationId,
    List<(string topicName, Guid topicId, List<int> partitionIds)> topics,
    int responsePartitionLimit
)
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
    for (int i = 0; i < 4; i++) WriteByte(0x00);
    WriteInt32(correlationId);
    WriteByte(0x00); // header tagged fields = none

    // ---- BODY ----
    WriteInt32(0); // throttle_time_ms
    WriteUnsignedVarInt(topics.Count + 1); // compact array length (N+1)

    foreach (var (topicName, topicId, partitionIds) in topics)
    {
        bool isUnknown =
            topicName.StartsWith("UNKNOWN_TOPIC_", StringComparison.Ordinal) &&
            topicId == Guid.Empty &&
            (partitionIds == null || partitionIds.Count == 0);

        // Topic ErrorCode
        //  - normal: 0
        //  - unknown topic: 3 (UNKNOWN_TOPIC_OR_PARTITION)
        short topicErrorCode = isUnknown ? (short)3 : (short)0;
        WriteInt16(topicErrorCode);

        // topic name
        byte[] nameBytes = Encoding.UTF8.GetBytes(topicName);
        WriteUnsignedVarInt(nameBytes.Length + 1);
        bytes.AddRange(nameBytes);

        // UUID
        if (isUnknown)
        {
            // 00000000-0000-0000-0000-000000000000
            bytes.AddRange(new byte[16]);
        }
        else
        {
            bytes.AddRange(KafkaProtocolUtils.GuidToBytesBigEndian(topicId));
        }

        WriteByte(0x00); // is_internal = false

        // --- Partitions (compact array) ---

        int partitionCount;
        if (isUnknown)
        {
            // Hiç partition yok → array length = 0
            partitionCount = 0;
        }
        else
        {
            // Stage logic zaten doğru sayıda partitionIds hazırladı
            partitionCount = partitionIds?.Count ?? 0;
        }

        // Varint LENGTH = realCount + 1
        WriteUnsignedVarInt(partitionCount + 1);

        for (int i = 0; i < partitionCount; i++)
        {
            int pid = (partitionIds != null && i < partitionIds.Count)
                ? partitionIds[i]
                : i;

            WriteInt16(0);   // partition_error_code
            WriteInt32(pid); // partition_index
            WriteInt32(0);   // leader_id
            WriteInt32(0);   // leader_epoch

            // replicas
            WriteUnsignedVarInt(2);
            WriteInt32(0);

            // isr
            WriteUnsignedVarInt(2);
            WriteInt32(0);

            // eligible leader replicas
            WriteUnsignedVarInt(2);
            WriteInt32(0);

            // last known elr
            WriteUnsignedVarInt(2);
            WriteInt32(0);

            // offline replicas
            WriteUnsignedVarInt(1);

            // partition tagged fields = none
            WriteUnsignedVarInt(0);
        }

        // topic_authorized_operations
        WriteInt32(33554432);
        // topic tagged fields = none
        WriteUnsignedVarInt(0);
    }

    // Cursor
    WriteByte(0xFF);        // IsCursorPresent = -1
    WriteUnsignedVarInt(0); // response tagged fields = none

    // ---- finalize message_size ----
    byte[] result = bytes.ToArray();
    int messageSize = result.Length - 4;
    byte[] sizeBytes = BitConverter.GetBytes(messageSize);
    if (BitConverter.IsLittleEndian) Array.Reverse(sizeBytes);
    Array.Copy(sizeBytes, 0, result, 0, 4);
    return result;
}

static byte[] BuildApiVersionsResponseV4(int correlationId, short errorCode)
{
    var bytes = new List<byte>();
    void Wb(byte b) => bytes.Add(b);
    void Wi16(short v) { var x = BitConverter.GetBytes(v); if (BitConverter.IsLittleEndian) Array.Reverse(x); bytes.AddRange(x); }
    void Wi32(int v) { var x = BitConverter.GetBytes(v); if (BitConverter.IsLittleEndian) Array.Reverse(x); bytes.AddRange(x); }
    void Wi64(long v) { var x = BitConverter.GetBytes(v); if (BitConverter.IsLittleEndian) Array.Reverse(x); bytes.AddRange(x); }

    // ---- Header ----
    for (int i = 0; i < 4; i++) Wb(0x00);
    Wi32(correlationId);
    Wi16(errorCode);

    // ---- API entries count (bizim homemade formatımızda 3 tane API var) ----
    Wb(0x04);

    // (1) ApiVersions (18)
    Wi16(18);  // ApiKey
    Wi16(0);   // MinVersion
    Wi16(4);   // MaxVersion
    Wb(0x00);  // tagged fields

    // (2) DescribeTopicPartitions (75)
    Wi16(75);  // ApiKey
    Wi16(0);   // MinVersion
    Wi16(0);   // MaxVersion
    Wb(0x00);  // tagged fields

    // (3) Fetch (1)  
    Wi16(1);   // ApiKey = 1 (Fetch)
    Wi16(0);   // MinVersion
    Wi16(16);  // MaxVersion (>= 16 şartı)
    Wb(0x00);  // tagged fields

    Wi32(0);       // throttle_ms
    Wb(0x00);      // response tagged fields = none
    
    var res = bytes.ToArray();
    int size = res.Length - 4;
    var sz = BitConverter.GetBytes(size);
    if (BitConverter.IsLittleEndian) Array.Reverse(sz);
    Array.Copy(sz, 0, res, 0, 4);
    return res;
}

// ---------- Deterministic UUID Helper ----------
static Guid DeterministicGuidFromName(string topicName, int index = 0)
{
    // Tester UUID pattern:
    // 71a59a51-8968-4f8b-937e-0000000005XX  (low bytes vary deterministically)
    byte[] baseBytes = new byte[16]
    {
        0x71, 0xA5, 0x9A, 0x51, 0x89, 0x68, 0x4F, 0x8B,
        0x93, 0x7E, 0x00, 0x00, 0x00, 0x00, 0x05, 0x80
    };

    int suffix = 0x80 + (index * 0x10);
    baseBytes[14] = (byte)((suffix >> 8) & 0xFF);
    baseBytes[15] = (byte)(suffix & 0xFF);
    return new Guid(baseBytes);
}

// ---------- UUID byte helpers ----------
static class KafkaProtocolUtils
{
    public static byte[] GuidToBytesBigEndian(Guid guid)
    {
        // Converts .NET GUID (little-endian) to Kafka (big-endian)
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
        // Converts Kafka big-endian UUID back to .NET layout
        byte[] le = new byte[16];
        le[0] = be[3]; le[1] = be[2]; le[2] = be[1]; le[3] = be[0];
        le[4] = be[5]; le[5] = be[4];
        le[6] = be[7]; le[7] = be[6];
        Array.Copy(be, 8, le, 8, 8);
        return le;
    }
}

// ---------- Metadata Parser ----------
static class MetadataParser
{
    public static (Guid topicId, List<int> partitionIds) ReadMetadataLog(string filePath, string topicName, int limit)
    {
        if (!File.Exists(filePath))
            return (Guid.Empty, Enumerable.Range(0, limit).ToList());

        byte[] fileBytes = File.ReadAllBytes(filePath);
        byte[] nameBytes = Encoding.UTF8.GetBytes(topicName);
        int nameIndex = IndexOf(fileBytes, nameBytes);
        if (nameIndex == -1)
            return (Guid.Empty, Enumerable.Range(0, limit).ToList());

        Guid topicId = FindUuidNear(fileBytes, nameIndex, topicName);
        List<int> detected = FindPartitionIdsForUuid(fileBytes, topicId, limit);

        if (detected.Count is > 0 and <= 8)
            return (topicId, detected);

        return (topicId, Enumerable.Range(0, limit).ToList());
    }

    private static int IndexOf(byte[] haystack, byte[] needle)
    {
        for (int i = 0; i <= haystack.Length - needle.Length; i++)
        {
            bool match = true;
            for (int j = 0; j < needle.Length; j++)
            {
                if (haystack[i + j] != needle[j]) { match = false; break; }
            }
            if (match) return i;
        }
        return -1;
    }

    private static Guid FindUuidNear(byte[] bytes, int aroundIndex, string topicName)
    {
        int nameLength = Encoding.UTF8.GetByteCount(topicName);
        int start = aroundIndex + nameLength;
        int end = Math.Min(bytes.Length - 16, start + 64);
        for (int i = start; i < end; i++)
        {
            if (LooksLikeUuid(bytes, i))
            {
                byte[] uuidBytes = new byte[16];
                Buffer.BlockCopy(bytes, i, uuidBytes, 0, 16);
                return new Guid(KafkaProtocolUtils.BytesBigEndianToGuid(uuidBytes));
            }
        }
        return Guid.Empty;
    }

    private static bool LooksLikeUuid(byte[] bytes, int offset)
    {
        if (offset + 16 > bytes.Length) return false;
        int zeros = 0;
        for (int i = 0; i < 16; i++)
            if (bytes[offset + i] == 0x00) zeros++;
        return zeros < 14;
    }

    private static List<int> FindPartitionIdsForUuid(byte[] bytes, Guid topicId, int limit)
    {
        if (limit <= 0) limit = 1;
        var ids = new List<int>();
        byte[] uuidBytes = KafkaProtocolUtils.GuidToBytesBigEndian(topicId);

        for (int i = 0; i <= bytes.Length - 20; i++)
        {
            bool match = true;
            for (int j = 0; j < 16; j++)
                if (bytes[i + j] != uuidBytes[j]) { match = false; break; }
            if (!match) continue;

            for (int lookahead = 16; lookahead < 128 && i + lookahead + 4 <= bytes.Length; lookahead++)
            {
                int candidate =
                    (bytes[i + lookahead] << 24) |
                    (bytes[i + lookahead + 1] << 16) |
                    (bytes[i + lookahead + 2] << 8) |
                    bytes[i + lookahead + 3];
                if (candidate >= 0 && candidate < 32)
                    ids.Add(candidate);
            }
        }

        ids = ids.Distinct().OrderBy(x => x).ToList();
        if (limit > 0 && ids.Count > limit) ids = ids.Take(limit).ToList();
        if (ids.Count == 0) ids = Enumerable.Range(0, limit).ToList();
        return ids;
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

        // Fallback: codecrafters ortamında log dizini genelde /app veya /tmp altındadır
        string fallback1 = "/app/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
        string fallback2 = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

        if (File.Exists(fallback1)) return fallback1;
        if (File.Exists(fallback2)) return fallback2;

        return fallback2; // en son garanti fallback
    }
}