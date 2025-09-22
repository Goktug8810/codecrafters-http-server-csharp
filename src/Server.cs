using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.IO;

// --- --directory flag'ini al ---
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
    TcpClient tcpClient = await server.AcceptTcpClientAsync(); // non-blocking accept
    tcpClient.NoDelay = true; // küçük cevapları hemen yolla
    _ = HandleClientAsync(tcpClient, baseDirFull); // her bağlantı ayrı async
}

static async Task HandleClientAsync(TcpClient tcpClient, string? baseDirFull)
{
    using (tcpClient)
    using (var stream = tcpClient.GetStream())
    {
        string raw = await ReadUntilHeadersEndAsync(stream);

        int endOfRequestLine = raw.IndexOf("\r\n");
        int endOfHeaders     = raw.IndexOf("\r\n\r\n");

        string requestLine = endOfRequestLine >= 0 ? raw[..endOfRequestLine] : raw;
        string[] parts = requestLine.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);

        string path = parts.Length >= 2 ? parts[1] : "/";

        // --- headers ---
        string headersSection = "";
        if (endOfHeaders > endOfRequestLine + 2)
        {
            headersSection = raw.Substring(
                endOfRequestLine + 2,
                endOfHeaders - (endOfRequestLine + 2));
        }

        string? userAgent = null;
        if (!string.IsNullOrEmpty(headersSection))
        {
            foreach (var line in headersSection.Split("\r\n"))
            {
                int colon = line.IndexOf(':');
                if (colon <= 0) continue;
                string name  = line[..colon].Trim();
                string value = line[(colon + 1)..].Trim();
                if (name.Equals("User-Agent", StringComparison.OrdinalIgnoreCase))
                {
                    userAgent = value;
                    break;
                }
            }
        }

        // --- routing ---
        if (path == "/")
        {
            await WriteAsciiAsync(stream, "HTTP/1.1 200 OK\r\n\r\n");
            return;
        }
        else if (path.StartsWith("/echo/"))
        {
            string body = path.Substring("/echo/".Length);
            int len = Encoding.ASCII.GetByteCount(body);
            string header =
                "HTTP/1.1 200 OK\r\n" +
                "Content-Type: text/plain\r\n" +
                $"Content-Length: {len}\r\n" +
                "\r\n";
            await WriteAsciiAsync(stream, header);
            await WriteAsciiAsync(stream, body);
            return;
        }
        else if (path == "/user-agent")
        {
            string body = userAgent ?? "";
            int len = Encoding.ASCII.GetByteCount(body);
            string header =
                "HTTP/1.1 200 OK\r\n" +
                "Content-Type: text/plain\r\n" +
                $"Content-Length: {len}\r\n" +
                "\r\n";
            await WriteAsciiAsync(stream, header);
            await WriteAsciiAsync(stream, body);
            return;
        }
        else if (path.StartsWith("/files/") && baseDirFull != null)
        {
            // --- /files/{filename} ---
            string fileName = path.Substring("/files/".Length); // {filename}

            if (string.IsNullOrEmpty(fileName))
            {
                await WriteAsciiAsync(stream, "HTTP/1.1 404 Not Found\r\n\r\n");
                return;
            }

            string fullPath = Path.GetFullPath(Path.Combine(baseDirFull, fileName));
            var baseNormalized = baseDirFull.TrimEnd(Path.DirectorySeparatorChar);
            var baseWithSep   = baseNormalized + Path.DirectorySeparatorChar;

            if (!fullPath.StartsWith(baseWithSep, StringComparison.Ordinal))
            {
                await WriteAsciiAsync(stream, "HTTP/1.1 404 Not Found\r\n\r\n");
                return;
            }

            if (!File.Exists(fullPath))
            {
                await WriteAsciiAsync(stream, "HTTP/1.1 404 Not Found\r\n\r\n");
                return;
            }

            byte[] fileBytes = await File.ReadAllBytesAsync(fullPath);
            string header =
                "HTTP/1.1 200 OK\r\n" +
                "Content-Type: application/octet-stream\r\n" +
                $"Content-Length: {fileBytes.Length}\r\n" +
                "\r\n";

            await WriteAsciiAsync(stream, header);
            await stream.WriteAsync(fileBytes, 0, fileBytes.Length);
            await stream.FlushAsync();
            return;
        }
        else
        {
            await WriteAsciiAsync(stream, "HTTP/1.1 404 Not Found\r\n\r\n");
            return;
        }
    }
}

static async Task<string> ReadUntilHeadersEndAsync(NetworkStream stream)
{
    var sb = new StringBuilder();
    var buffer = new byte[1024];
    while (true)
    {
        int n = await stream.ReadAsync(buffer, 0, buffer.Length);
        if (n <= 0) break; // bağlantı kapandı
        sb.Append(Encoding.ASCII.GetString(buffer, 0, n));
        if (sb.ToString().Contains("\r\n\r\n")) break;
    }
    return sb.ToString();
}

static Task WriteAsciiAsync(NetworkStream stream, string s)
{
    byte[] bytes = Encoding.ASCII.GetBytes(s);
    return stream.WriteAsync(bytes, 0, bytes.Length);
}