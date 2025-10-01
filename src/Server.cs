using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.IO.Compression;

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
        // --- headers ---

        while (true)
        {
            string raw = await ReadUntilHeadersEndAsync(stream);

            int endOfRequestLine = raw.IndexOf("\r\n");
            int endOfHeaders     = raw.IndexOf("\r\n\r\n");

            // --- request line ---
            string requestLine = endOfRequestLine >= 0 ? raw[..endOfRequestLine] : raw;
            string[] parts = requestLine.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);
            string method = parts.Length >= 1 ? parts[0] : "GET";
            string path   = parts.Length >= 2 ? parts[1] : "/";

            // --- headers section (string) ---
            string headersSection = "";
            if (endOfHeaders > endOfRequestLine + 2)
            {
                headersSection = raw.Substring(
                    endOfRequestLine + 2,
                    endOfHeaders - (endOfRequestLine + 2));
            }

            string? userAgent = null;
            string? contentLengthRaw = null;
            string? contentType = null;
            string? acceptEncoding = null;
            string? connectionHeader = null;

            if (!string.IsNullOrEmpty(headersSection))
            {
                foreach (var line in headersSection.Split("\r\n"))
                {
                    int colon = line.IndexOf(':');
                    if (colon <= 0) continue;

                    string name  = line[..colon].Trim();
                    string value = line[(colon + 1)..].Trim();

                    if (name.Equals("User-Agent", StringComparison.OrdinalIgnoreCase))
                        userAgent = value;

                    if (name.Equals("Content-Length", StringComparison.OrdinalIgnoreCase))
                        contentLengthRaw = value;

                    if (name.Equals("Content-Type", StringComparison.OrdinalIgnoreCase))
                        contentType = value;
                    
                    if (name.Equals("Accept-Encoding", StringComparison.OrdinalIgnoreCase))
                        acceptEncoding = value;
                    
                    if (name.Equals("Connection", StringComparison.OrdinalIgnoreCase))
                        connectionHeader = value;
                }
            }

            // --- body hazırla: header-sonrası pakette gelen ilk parça ---
            string bodySoFarText = raw[(endOfHeaders + 4)..];                 // header'dan sonra gelen kısmın text hali
            byte[] firstBytes    = Encoding.ASCII.GetBytes(bodySoFarText);    // bu stage'de ASCII yeterli
            int?   contentLength = null;
            if (contentLengthRaw != null && int.TryParse(contentLengthRaw, out var length))
                contentLength = length;

            byte[]? bodyBuffer = null; // tam body (byte[]) burada tutulacak

            // Sadece body gereken durumlarda tamamlayacağız (örn. POST /files/..)
            // Ama body uzunluğunu ve ilk parçayı şimdiden hazırlamak faydalı.
            if (contentLength.HasValue && contentLength.Value >= 0)
            {
                int target = contentLength.Value;
                if (target > 0)
                {
                    bodyBuffer = new byte[target];

                    // İlk parça (ilk okuma paketinde header'dan sonra gelen baytlar)
                    int filled = Math.Min(firstBytes.Length, target);
                    if (filled > 0)
                        Buffer.BlockCopy(firstBytes, 0, bodyBuffer, 0, filled);

                    // Kalanı stream'den oku
                    while (filled < target)
                    {
                        int n = await stream.ReadAsync(bodyBuffer, filled, target - filled);
                        if (n <= 0) break; // bağlantı kapandı/erken bitti -> protokol ihlali sayılabilir
                        filled += n;
                    }

                    // Not: Eğer filled < target kaldıysa, body eksik geldi demektir.
                    // Bu stage'de basitçe devam edip eksikse de yazmamak tercih edebiliriz.
                }
                else
                {
                    // Content-Length: 0 -> boş body
                    bodyBuffer = Array.Empty<byte>();
                }
            }

            // --- routing ---
            if (path == "/")
            {
                await WriteAsciiAsync(stream, "HTTP/1.1 200 OK\r\n\r\n");
                
                if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                    break;
                else
                    continue;
            }
            //else if (path.StartsWith("/echo/"))
            //{
              //  string body = path.Substring("/echo/".Length);
               // int len = Encoding.ASCII.GetByteCount(body);
                //string header =
                  //  "HTTP/1.1 200 OK\r\n" +
                   // "Content-Type: text/plain\r\n" +
                    //$"Content-Length: {len}\r\n" +
                    //"\r\n";
                
                //if (acceptEncoding?.Contains("gzip") == true)
                  //  header += "Content-Encoding: gzip\r\n";
                
              //  header += "\r\n";

                //await WriteAsciiAsync(stream, header);
                //await WriteAsciiAsync(stream, body);
                //return;
            //}
            else if (path.StartsWith("/echo/"))
            {
                string plainText = path.Substring("/echo/".Length);

                if (acceptEncoding?.Contains("gzip") == true)
                {
                    // Gzip compression
                    byte[] inputBytes = Encoding.UTF8.GetBytes(plainText);
                    using var outputStream = new MemoryStream();
                    using (var gzip = new GZipStream(outputStream, CompressionLevel.SmallestSize, leaveOpen: true))
                    {
                        gzip.Write(inputBytes, 0, inputBytes.Length);
                    }
                    byte[] compressed = outputStream.ToArray();

                    string header =
                        "HTTP/1.1 200 OK\r\n" +
                        "Content-Type: text/plain\r\n" +
                        "Content-Encoding: gzip\r\n" +
                        $"Content-Length: {compressed.Length}\r\n";
                    
                    header += "\r\n";
                    
                    if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                        header += "Connection: close\r\n";
                    
                    await WriteAsciiAsync(stream, header);
                    await stream.WriteAsync(compressed, 0, compressed.Length);
                    if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                        break;
                    else
                        continue;;
                }
                else
                {
                    // Gzip desteklenmiyorsa düz metin
                    int len = Encoding.ASCII.GetByteCount(plainText);
                    string header =
                        "HTTP/1.1 200 OK\r\n" +
                        "Content-Type: text/plain\r\n" +
                        $"Content-Length: {len}\r\n";
                    
                    if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                        header += "Connection: close\r\n";
                    
                        header += "\r\n";

                    await WriteAsciiAsync(stream, header);
                    await WriteAsciiAsync(stream, plainText);
                    if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                        break;
                    else
                        continue;;
                }
            }
            else if (path == "/user-agent")
            {
                string body = userAgent ?? "";
                int len = Encoding.ASCII.GetByteCount(body);
                string header =
                    "HTTP/1.1 200 OK\r\n" +
                    "Content-Type: text/plain\r\n" +
                    $"Content-Length: {len}\r\n";
                
                header += "\r\n";
                
                if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                    header += "Connection: close\r\n";
                await WriteAsciiAsync(stream, header);
                await WriteAsciiAsync(stream, body);
                if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                    break;
                else
                    continue;;
            }
            else if (path.StartsWith("/files/") && baseDirFull != null)
            {
                string fileName = path.Substring("/files/".Length); // {filename}
                if (string.IsNullOrEmpty(fileName))
                {
                    await WriteAsciiAsync(stream, "HTTP/1.1 404 Not Found\r\n\r\n");
                    if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                        break;
                    else
                        continue;;
                }

                // path traversal guard
                string fullPath = Path.GetFullPath(Path.Combine(baseDirFull, fileName));
                var baseNormalized = baseDirFull.TrimEnd(Path.DirectorySeparatorChar);
                var baseWithSep    = baseNormalized + Path.DirectorySeparatorChar;
                if (!fullPath.StartsWith(baseWithSep, StringComparison.Ordinal))
                {
                    await WriteAsciiAsync(stream, "HTTP/1.1 404 Not Found\r\n\r\n");
                    if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                        break;
                    else
                        continue;;
                }

                // --- Method bazlı ayrım: GET = oku, POST = yaz ---
                if (method.Equals("GET", StringComparison.OrdinalIgnoreCase))
                {
                    if (!File.Exists(fullPath))
                    {
                        await WriteAsciiAsync(stream, "HTTP/1.1 404 Not Found\r\n\r\n");
                        if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                            break;
                        else
                            continue;;
                    }

                    byte[] fileBytes = await File.ReadAllBytesAsync(fullPath);
                    string header =
                        "HTTP/1.1 200 OK\r\n" +
                        "Content-Type: application/octet-stream\r\n" +
                        $"Content-Length: {fileBytes.Length}\r\n";
                    
                    if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                        header += "Connection: close\r\n";
                    
                    header += "\r\n";
                    await WriteAsciiAsync(stream, header);
                    await stream.WriteAsync(fileBytes, 0, fileBytes.Length);
                    await stream.FlushAsync();
                    if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                        break;
                    else
                        continue;;
                }
                else if (method.Equals("POST", StringComparison.OrdinalIgnoreCase))
                {
                    // Bu stage’de: body zorunlu, Content-Length zorunlu
                    if (contentLength is null || contentLength.Value < 0 || bodyBuffer is null)
                    {
                        await WriteAsciiAsync(stream, "HTTP/1.1 400 Bad Request\r\n\r\n");
                        if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                            break;
                        else
                            continue;;
                    }

                    // Dosyaya "ham" body yaz (text/binary fark etmez)
                    // Klasör yoksa (tester oluşturuyor ama gene de), güvenli olmak için:
                    Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);

                    await File.WriteAllBytesAsync(fullPath, bodyBuffer);

                    // Stage beklentisi: 201 Created, header/body zorunlu değil.
                    await WriteAsciiAsync(stream, "HTTP/1.1 201 Created\r\n\r\n");
                    if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                        break;
                    else
                        continue;
                }
                else
                {
                    // Bu endpoint için sadece GET/POST destekliyoruz.
                    await WriteAsciiAsync(stream, "HTTP/1.1 405 Method Not Allowed\r\n\r\n");
                    if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                        break;
                    else
                        continue;
                }
            }
            else
            {
                await WriteAsciiAsync(stream, "HTTP/1.1 404 Not Found\r\n\r\n");
                if (connectionHeader?.Equals("close", StringComparison.OrdinalIgnoreCase) == true)
                    break;
                else
                    continue;
            }
            
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