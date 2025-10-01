using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

// -----------------------------
// Boot
// -----------------------------
string? baseDir = null;
for (int i = 0; i < args.Length; i++)
{
    if (args[i] == "--directory" && i + 1 < args.Length)
        baseDir = args[i + 1];
}
string? baseDirFull = baseDir is null ? null : Path.GetFullPath(baseDir);

var server = new TcpListener(IPAddress.Any, 4221);
server.Start();

while (true)
{
    var tcp = await server.AcceptTcpClientAsync().ConfigureAwait(false);
    tcp.NoDelay = true;
    _ = MiniHttpServer.HandleClientAsync(tcp, baseDirFull);
}

// -----------------------------
// Core Server
// -----------------------------
static class MiniHttpServer
{
    public static async Task HandleClientAsync(TcpClient tcpClient, string? baseDirFull)
    {
        using (tcpClient)
        using (var stream = tcpClient.GetStream())
        {
            var reader = new HttpReader(stream);
            var writer = new HttpWriter(stream);

            while (true)
            {
                // 1) Read & parse request
                var read = await reader.ReadRequestAsync().ConfigureAwait(false);
                if (read is null) break; // client closed

                HttpRequest req = read.Value.Request;
                byte[] body = read.Value.Body ?? Array.Empty<byte>();

                // 2) Route
                HttpResponse res = Router.Route(req, body, baseDirFull);

                // 3) Write response
                await writer.WriteAsync(res).ConfigureAwait(false);

                // 4) Connection lifetime
                if (req.ConnectionClose)
                    break;
            }
        }
    }
}

// -----------------------------
// HTTP Types
// -----------------------------
readonly struct HttpRequest
{
    public readonly string Method;
    public readonly string Path;
    public readonly string Version;
    public readonly HeaderBag Headers;
    public readonly int? ContentLength;
    public readonly string? UserAgent;
    public readonly string? AcceptEncoding;
    public readonly bool ConnectionClose;

    public HttpRequest(
        string method, string path, string version, HeaderBag headers)
    {
        Method = method;
        Path = path;
        Version = version;
        Headers = headers;

        if (headers.TryGet("Content-Length", out var cl) && int.TryParse(cl, out var len))
            ContentLength = len;
        else
            ContentLength = null;

        UserAgent = headers.TryGet("User-Agent", out var ua) ? ua : null;
        AcceptEncoding = headers.TryGet("Accept-Encoding", out var ae) ? ae : null;

        var conn = headers.TryGet("Connection", out var ch) ? ch : null;
        ConnectionClose = conn != null && conn.Equals("close", StringComparison.OrdinalIgnoreCase);
    }
}

sealed class HttpResponse
{
    public int StatusCode { get; set; } = 200;
    public string ReasonPhrase { get; set; } = "OK";
    public string ContentType { get; set; } = "text/plain";
    public Dictionary<string,string> Headers { get; } = new(StringComparer.OrdinalIgnoreCase);
    public byte[] Body { get; set; } = Array.Empty<byte>();

    public void SetHeader(string name, string value) => Headers[name] = value;
}

sealed class HeaderBag
{
    private readonly Dictionary<string, string> _map = new(StringComparer.OrdinalIgnoreCase);

    public void Add(string name, string value)
    {
        if (string.IsNullOrWhiteSpace(name)) return;
        _map[name.Trim()] = value?.Trim() ?? string.Empty;
    }

    public bool TryGet(string name, out string value) => _map.TryGetValue(name, out value!);
}

// -----------------------------
// Reader / Writer
// -----------------------------
sealed class HttpReader
{
    private readonly NetworkStream _stream;
    public HttpReader(NetworkStream stream) => _stream = stream;

    private static readonly byte[] CRLFCRLF = Encoding.ASCII.GetBytes("\r\n\r\n");

    public async Task<(HttpRequest Request, byte[]? Body)?> ReadRequestAsync()
    {
        // Read until CRLFCRLF; keep leftover for body
        var headerBuffer = new MemoryStream();
        var readBuffer = ArrayPool<byte>.Shared.Rent(2048);
        try
        {
            int headerEndAt = -1;
            byte[]? firstBodyChunk = null;

            while (true)
            {
                int n = await _stream.ReadAsync(readBuffer, 0, readBuffer.Length).ConfigureAwait(false);
                if (n <= 0)
                {
                    if (headerBuffer.Length == 0) return null; // client closed before any data
                    break;
                }

                headerBuffer.Write(readBuffer, 0, n);

                // Search for CRLFCRLF
                var span = new ReadOnlySpan<byte>(headerBuffer.GetBuffer(), 0, (int)headerBuffer.Length);
                int idx = IndexOf(span, CRLFCRLF);
                if (idx >= 0)
                {
                    headerEndAt = idx + CRLFCRLF.Length;
                    int leftover = span.Length - headerEndAt;
                    if (leftover > 0)
                    {
                        firstBodyChunk = new byte[leftover];
                        Array.Copy(headerBuffer.GetBuffer(), headerEndAt, firstBodyChunk, 0, leftover);
                    }
                    break;
                }
            }

            if (headerEndAt < 0)
                return null; // malformed or closed

            // Parse request line + headers (ASCII)
            var headerBytes = headerBuffer.GetBuffer();
            string headerText = Encoding.ASCII.GetString(headerBytes, 0, headerEndAt);
            var (request, contentLength) = ParseHeaders(headerText);

            // Read body if Content-Length present
            byte[]? body = null;
            if (contentLength.HasValue && contentLength.Value >= 0)
            {
                int target = contentLength.Value;
                body = new byte[target];
                int filled = 0;

                if (firstBodyChunk is not null && firstBodyChunk.Length > 0)
                {
                    int toCopy = Math.Min(firstBodyChunk.Length, target);
                    Buffer.BlockCopy(firstBodyChunk, 0, body, 0, toCopy);
                    filled += toCopy;
                }

                while (filled < target)
                {
                    int n = await _stream.ReadAsync(body, filled, target - filled).ConfigureAwait(false);
                    if (n <= 0) break; // premature close
                    filled += n;
                }

                if (filled < target)
                {
                    // Body incomplete -> we still return what we have (aligning with original minimal server behavior)
                    if (filled == 0) body = Array.Empty<byte>();
                    else if (filled < target)
                    {
                        var shrunk = new byte[filled];
                        Buffer.BlockCopy(body, 0, shrunk, 0, filled);
                        body = shrunk;
                    }
                }
            }

            return (request, body);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(readBuffer);
        }
    }

    private static (HttpRequest Request, int? ContentLength) ParseHeaders(string headersText)
    {
        // request line
        int firstCrlf = headersText.IndexOf("\r\n", StringComparison.Ordinal);
        string reqLine = firstCrlf >= 0 ? headersText[..firstCrlf] : headersText;

        var parts = reqLine.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);
        string method = parts.Length > 0 ? parts[0] : "GET";
        string path   = parts.Length > 1 ? parts[1] : "/";
        string ver    = parts.Length > 2 ? parts[2] : "HTTP/1.1";

        var bag = new HeaderBag();
        if (firstCrlf >= 0 && firstCrlf + 2 < headersText.Length)
        {
            string lines = headersText.Substring(firstCrlf + 2);
            int endOfHeaderBlock = lines.IndexOf("\r\n\r\n", StringComparison.Ordinal);
            if (endOfHeaderBlock >= 0) lines = lines[..endOfHeaderBlock];

            foreach (var line in lines.Split("\r\n"))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                int colon = line.IndexOf(':');
                if (colon <= 0) continue;
                string name = line[..colon].Trim();
                string value = line[(colon + 1)..].Trim();
                bag.Add(name, value);
            }
        }

        var req = new HttpRequest(method, path, ver, bag);
        return (req, req.ContentLength);
    }

    private static int IndexOf(ReadOnlySpan<byte> haystack, ReadOnlySpan<byte> needle)
    {
        if (needle.Length == 0) return 0;
        for (int i = 0; i <= haystack.Length - needle.Length; i++)
        {
            if (haystack[i] == needle[0] && haystack.Slice(i, needle.Length).SequenceEqual(needle))
                return i;
        }
        return -1;
    }
}

sealed class HttpWriter
{
    private readonly NetworkStream _stream;
    public HttpWriter(NetworkStream stream) => _stream = stream;

    public async Task WriteAsync(HttpResponse res)
    {
        // Status line
        var sb = new StringBuilder();
        sb.Append("HTTP/1.1 ").Append(res.StatusCode).Append(' ').Append(res.ReasonPhrase).Append("\r\n");

        // Standard headers
        if (!res.Headers.ContainsKey("Content-Type"))
            sb.Append("Content-Type: ").Append(res.ContentType).Append("\r\n");

        sb.Append("Content-Length: ").Append(res.Body?.Length ?? 0).Append("\r\n");

        // Custom headers
        foreach (var kv in res.Headers)
        {
            if (kv.Key.Equals("Content-Type", StringComparison.OrdinalIgnoreCase)) continue; // already wrote
            if (kv.Key.Equals("Content-Length", StringComparison.OrdinalIgnoreCase)) continue; // already wrote
            sb.Append(kv.Key).Append(": ").Append(kv.Value).Append("\r\n");
        }

        // End of headers
        sb.Append("\r\n");

        // Write headers + body
        var headerBytes = Encoding.ASCII.GetBytes(sb.ToString());
        await _stream.WriteAsync(headerBytes, 0, headerBytes.Length).ConfigureAwait(false);

        if (res.Body is { Length: > 0 })
            await _stream.WriteAsync(res.Body, 0, res.Body.Length).ConfigureAwait(false);

        await _stream.FlushAsync().ConfigureAwait(false);
    }
}

// -----------------------------
// Router & Handlers
// -----------------------------
static class Router
{
    public static HttpResponse Route(HttpRequest req, byte[] body, string? baseDirFull)
    {
        // "/" -> 200, empty body
        if (req.Path == "/")
            return Responses.OkEmpty(req);

        // "/echo/{text}"
        if (req.Path.StartsWith("/echo/", StringComparison.Ordinal))
            return EchoHandler.Handle(req);

        // "/user-agent"
        if (req.Path == "/user-agent")
            return UserAgentHandler.Handle(req);

        // "/files/{filename}" (GET/POST), only when base dir provided
        if (req.Path.StartsWith("/files/", StringComparison.Ordinal) && baseDirFull != null)
            return FilesHandler.Handle(req, body, baseDirFull);

        return Responses.NotFound(req);
    }
}

static class EchoHandler
{
    public static HttpResponse Handle(HttpRequest req)
    {
        string text = req.Path.Substring("/echo/".Length);
        // Encode as UTF8 for body; tests typically use ASCII-safe text
        var plain = Encoding.UTF8.GetBytes(text);

        // Gzip?
        bool wantsGzip = req.AcceptEncoding?.Contains("gzip", StringComparison.OrdinalIgnoreCase) == true;
        if (wantsGzip)
        {
            using var ms = new MemoryStream();
            using (var gz = new GZipStream(ms, CompressionLevel.SmallestSize, leaveOpen: true))
            {
                gz.Write(plain, 0, plain.Length);
            }
            var compressed = ms.ToArray();

            var res = Responses.OkBinary("text/plain", compressed);
            res.SetHeader("Content-Encoding", "gzip");
            if (req.ConnectionClose) res.SetHeader("Connection", "close");
            return res;
        }
        else
        {
            var res = Responses.OkBinary("text/plain", plain);
            if (req.ConnectionClose) res.SetHeader("Connection", "close");
            return res;
        }
    }
}

static class UserAgentHandler
{
    public static HttpResponse Handle(HttpRequest req)
    {
        var body = Encoding.ASCII.GetBytes(req.UserAgent ?? "");
        var res = Responses.OkBinary("text/plain", body);
        if (req.ConnectionClose) res.SetHeader("Connection", "close");
        return res;
    }
}

static class FilesHandler
{
    public static HttpResponse Handle(HttpRequest req, byte[] rawBody, string baseDirFull)
    {
        string fileName = req.Path.Substring("/files/".Length);
        if (string.IsNullOrEmpty(fileName))
            return Responses.NotFound(req);

        string baseNorm = baseDirFull.TrimEnd(Path.DirectorySeparatorChar);
        string baseWithSep = baseNorm + Path.DirectorySeparatorChar;

        string fullPath = Path.GetFullPath(Path.Combine(baseDirFull, fileName));

        // path traversal guard
        if (!fullPath.StartsWith(baseWithSep, StringComparison.Ordinal))
            return Responses.NotFound(req);

        if (req.Method.Equals("GET", StringComparison.OrdinalIgnoreCase))
        {
            if (!File.Exists(fullPath))
                return Responses.NotFound(req);

            byte[] fileBytes = File.ReadAllBytes(fullPath);
            var res = Responses.OkBinary("application/octet-stream", fileBytes);
            if (req.ConnectionClose) res.SetHeader("Connection", "close");
            return res;
        }
        else if (req.Method.Equals("POST", StringComparison.OrdinalIgnoreCase))
        {
            // Content-Length must be present & body already read by reader
            if (req.ContentLength is null || req.ContentLength.Value < 0)
                return Responses.BadRequest(req);

            Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
            File.WriteAllBytes(fullPath, rawBody);

            var res = new HttpResponse { StatusCode = 201, ReasonPhrase = "Created", ContentType = "text/plain", Body = Array.Empty<byte>() };
            if (req.ConnectionClose) res.SetHeader("Connection", "close");
            return res;
        }
        else
        {
            return Responses.MethodNotAllowed(req);
        }
    }
}

static class Responses
{
    public static HttpResponse OkEmpty(HttpRequest req)
        => new HttpResponse { StatusCode = 200, ReasonPhrase = "OK", ContentType = "text/plain", Body = Array.Empty<byte>() };

    public static HttpResponse OkBinary(string contentType, byte[] body)
        => new HttpResponse { StatusCode = 200, ReasonPhrase = "OK", ContentType = contentType, Body = body };

    public static HttpResponse NotFound(HttpRequest req)
        => new HttpResponse { StatusCode = 404, ReasonPhrase = "Not Found", ContentType = "text/plain", Body = Array.Empty<byte>() };

    public static HttpResponse BadRequest(HttpRequest req)
        => new HttpResponse { StatusCode = 400, ReasonPhrase = "Bad Request", ContentType = "text/plain", Body = Array.Empty<byte>() };

    public static HttpResponse MethodNotAllowed(HttpRequest req)
        => new HttpResponse { StatusCode = 405, ReasonPhrase = "Method Not Allowed", ContentType = "text/plain", Body = Array.Empty<byte>() };
}