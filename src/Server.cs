using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

var server = new TcpListener(IPAddress.Any, 4221);
server.Start();

while (true)
{
    TcpClient tcpClient = await server.AcceptTcpClientAsync(); // non-blocking accept
    tcpClient.NoDelay = true; //kucuk cevapları hemen yolla
    _ = HandleClientAsync(tcpClient); // fire-and-forget; her bağlantı ayrı async akışta
}

static async Task HandleClientAsync(TcpClient tcpClient)
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

        string response;
        if (path == "/")
        {
            response = "HTTP/1.1 200 OK\r\n\r\n";
        }
        else if (path.StartsWith("/echo/"))
        {
            string body = path.Substring("/echo/".Length);
            int len = Encoding.ASCII.GetByteCount(body);
            response =
                "HTTP/1.1 200 OK\r\n" +
                "Content-Type: text/plain\r\n" +
                $"Content-Length: {len}\r\n" +
                "\r\n" +
                body;
        }
        else if (path == "/user-agent")
        {
            string body = userAgent ?? "";
            int len = Encoding.ASCII.GetByteCount(body);
            response =
                "HTTP/1.1 200 OK\r\n" +
                "Content-Type: text/plain\r\n" +
                $"Content-Length: {len}\r\n" +
                "\r\n" +
                body;
        }
        else
        {
            response = "HTTP/1.1 404 Not Found\r\n\r\n";
        }

        byte[] respBytes = Encoding.ASCII.GetBytes(response);
        await stream.WriteAsync(respBytes, 0, respBytes.Length);
        await stream.FlushAsync();
        // TcpClient using{} ile kapanacak
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