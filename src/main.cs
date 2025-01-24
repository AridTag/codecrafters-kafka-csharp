using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;

var server = new TcpListener(IPAddress.Any, 9092);
server.Start();

var receiveBuffer = new byte[1024];
var newClient = await server.AcceptTcpClientAsync().ConfigureAwait(false);
var networkStream = newClient.GetStream();

while (newClient.Connected)
{
    try
    {
        await networkStream.ReadExactlyAsync(receiveBuffer, 0, 4).ConfigureAwait(false);
    }
    catch (EndOfStreamException)
    {
        Console.WriteLine("Client disconnected while waiting for message size");
        break;
    }

    var messageSize = BinaryPrimitives.ReadInt32BigEndian(receiveBuffer.AsSpan(0, 4));
    if (messageSize > receiveBuffer.Length)
    {
        receiveBuffer = new byte[messageSize];
    }

    try
    {
        await networkStream.ReadExactlyAsync(receiveBuffer, 0, messageSize).ConfigureAwait(false);
    }
    catch (EndOfStreamException)
    {
        Console.WriteLine("Client disconnected while waiting for message");
        break;
    }

    SendResponse(networkStream, 7);
}

static void SendResponse(NetworkStream stream, int correlationId)
{
    var response = new PacketBuilder(stackalloc byte[8])
        .Write(correlationId)
        .Build();

    stream.Write(response);
    stream.Flush();
}

public ref struct PacketBuilder(Span<byte> initialBuffer)
{
    private Span<byte> _Buffer = initialBuffer;
    private int _Offset = 4; // Start with 4 bytes for the size

    public PacketBuilder Write(int value)
    {
        if (_Offset + sizeof(int) > _Buffer.Length)
        {
            var newBuffer = ArrayPool<byte>.Shared.Rent(_Buffer.Length * 2);
            _Buffer.CopyTo(newBuffer);
            _Buffer = newBuffer;
        }
        
        BinaryPrimitives.WriteInt32BigEndian(_Buffer[_Offset..], value);

        _Offset += sizeof(int);
        return this;
    }

    public Span<byte> Build()
    {
        BinaryPrimitives.WriteInt32BigEndian(_Buffer[..4], _Offset);
        
        return _Buffer.Slice(0, _Offset);
    }
}