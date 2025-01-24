using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

using var server = new TcpListener(IPAddress.Any, 9092);
server.Start();

var receiveBuffer = new byte[1024];
using var newClient = await server.AcceptTcpClientAsync().ConfigureAwait(false);
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

    var request = ReadRequest(receiveBuffer.AsSpan(0, messageSize));
    
    await SendResponse(networkStream, request).ConfigureAwait(false);
}

return;

static Request ReadRequest(ReadOnlySpan<byte> buffer)
{
    var requestApiKey = BinaryPrimitives.ReadInt16BigEndian(buffer);
    buffer = buffer[2..];
    var requestApiVersion = BinaryPrimitives.ReadInt16BigEndian(buffer);
    buffer = buffer[2..];
    var requestCorrelationId = BinaryPrimitives.ReadInt32BigEndian(buffer);

    return new()
    {
        ApiKey = requestApiKey,
        ApiVersion = requestApiVersion,
        CorrelationId = requestCorrelationId,
        ClientId = string.Empty
    };
}

static async Task SendResponse(NetworkStream stream, Request request)
{
    PacketData response;
    if (request.ApiVersion is < 0 or > 4)
    {
        response = new PacketBuilder()
            .Write(request.CorrelationId)
            .Write((short)35) // Invalid version
            .Build();
    }
    else
    {
        response = new PacketBuilder()
            .Write(request.CorrelationId)
            .Write((short)0)
            .Build();
    }

    await stream.WriteAsync(response.Buffer.AsMemory(0, response.Size)).ConfigureAwait(false);
    await stream.FlushAsync().ConfigureAwait(false);
}

public struct PacketData : IDisposable
{
    public byte[] Buffer;
    public int Size;
    
    public void Dispose()
    {
        ArrayPool<byte>.Shared.Return(Buffer);
        Buffer = null!;
        Size = 0;
    }
}

public ref struct PacketBuilder()
{
    private byte[] _Buffer = ArrayPool<byte>.Shared.Rent(1024);
    private int _Offset = 4; // Start with 4 bytes for the size

    private void EnsureCapacity(int size)
    {
        if (_Offset + size <= _Buffer.Length) return;
        
        var newBuffer = ArrayPool<byte>.Shared.Rent(_Buffer.Length * 2);
        _Buffer.CopyTo(newBuffer.AsSpan());
        _Buffer = newBuffer;
    }

    public PacketBuilder Write(int value)
    {
        EnsureCapacity(sizeof(int));
        
        BinaryPrimitives.WriteInt32BigEndian(_Buffer.AsSpan(_Offset), value);

        _Offset += sizeof(int);
        return this;
    }

    public PacketBuilder Write(short value)
    {
        EnsureCapacity(sizeof(short));
        
        BinaryPrimitives.WriteInt16BigEndian(_Buffer.AsSpan(_Offset), value);

        _Offset += sizeof(short);
        return this;
    }

    public PacketData Build()
    {
        BinaryPrimitives.WriteInt32BigEndian(_Buffer.AsSpan(), _Offset);
        
        return new ()
        {
            Buffer = _Buffer,
            Size = _Offset,
        };
    }
}

struct Request
{
    public short ApiKey;
    public short ApiVersion;
    public int CorrelationId;
    public string ClientId;
}