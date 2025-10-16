using System;
using System.Net;
using System.Net.Sockets;

// Kafka broker stub’ı başlat: TCP port 9092’de dinle
TcpListener server = new TcpListener(IPAddress.Any, 9092);
server.Start();
Console.WriteLine("Kafka broker stub running on port 9092...");

while (true)
{
    var client = server.AcceptSocket();
    Console.WriteLine("Client connected.");

    try
    {
        // Kafka request header’ının ilk 12 byte’ını oku (kafka contracts)
        //    - 0..3: message_size
        //    - 4..5: api_key
        //    - 6..7: api_version
        //    - 8..11: correlation_id  
        byte[] requestHeader = ReadExactly(client, 12);

        // Offset 8–11 arasındaki 4 byte’ı big-endian INT32 olarak çöz (kafka wire protocol spec.)
        int correlationId = ReadInt32BigEndian(requestHeader, 8);
        Console.WriteLine($"Received correlation_id: {correlationId}");

        // Response oluştur (8 byte)
        //    [0..3] message_size (tester sadece 4 byte uzunluk arıyor)
        //    [4..7] correlation_id (istekten aldığımız gerçek değer)
        byte[] response = new byte[8];
        WriteInt32BigEndian(response, 0, 0);               // message_size (önemli değil)
        WriteInt32BigEndian(response, 4, correlationId);   // correlation_id (istekten aynen geri)

        // Response’u client’a gönder
        client.Send(response);
        Console.WriteLine("Sent 8 bytes: " + BitConverter.ToString(response));
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

// TCP "stream"tir, yani gelen veriler paket paket değil akış halindedir.
// Tek bir Receive çağrısı genellikle istediğin kadar byte getirmez.
// Bu helper, hedef uzunluğa ulaşana kadar okumaya devam eder.
//Amaç: TCP’nin “partial read” sorununu önlemek.
// Kafka header’ı tam olarak 12 byte olduğundan,
// burada 12 byte gelene kadar bekler.
static byte[] ReadExactly(Socket socket, int length)
{
    byte[] buffer = new byte[length];
    int offset = 0;

    while (offset < length)
    {
        // Soketten veri al
        int read = socket.Receive(buffer, offset, length - offset, SocketFlags.None);
        // 0 dönüyorsa bağlantı kapandı (client erkenden kapattı)
        if (read == 0)
            throw new Exception("Connection closed before full header was received.");
        // Toplam okunan byte sayısını artır
        offset += read;
    }
    // Artık tam length kadar byte okundu
    return buffer;
}

//Amaç: Wire’daki (big-endian) integer’ı makinenin doğal endian’ına çevirmek.
// örnek: 6f 7f c6 61 → 1870644833
// Kafka big-endian kullanır (network byte order).
// C# sistemleri genellikle little-endian’dır.
// Bu helper, 4 byte’ı doğru endian’da INT32’ye çevirir.
static int ReadInt32BigEndian(byte[] buffer, int offset)
{
    byte[] temp = new byte[4];
    Array.Copy(buffer, offset, temp, 0, 4);

    // Eğer sistem little-endian ise byte sırasını ters çevir
    if (BitConverter.IsLittleEndian)
        Array.Reverse(temp);

    // Artık Big-Endian’dan doğru şekilde INT32 oluştur
    return BitConverter.ToInt32(temp, 0);
}

//Amaç: Kafka yanıtlarını protokol standardına uygun göndermek
//Response’ta 8 byte’lık veri: 00 00 00 00   (message_size)
// 6f 7f c6 61   (correlation_id)
// INT32 değeri 4 byte’a çevir, sonra network byte order (big-endian) olarak yaz.
static void WriteInt32BigEndian(byte[] buffer, int offset, int value)
{
    var bytes = BitConverter.GetBytes(value);

    // Sistem little-endian ise ters çevir (Kafka big-endian ister)
    if (BitConverter.IsLittleEndian)
        Array.Reverse(bytes);

    // Bu 4 byte’ı buffer’a kopyala (belirtilen offset’ten başlayarak)
    Array.Copy(bytes, 0, buffer, offset, 4);
}