using System.Net;
using System.Net.Sockets;

public static class Program
{
    public static void Main(string[] args)
    {
        var server = new TcpListener(IPAddress.Any, 9092);
        server.Start();
        server.AcceptSocket();
    }
}
