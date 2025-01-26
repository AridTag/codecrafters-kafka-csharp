using System;
using System.Buffers;
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading;

namespace kafka;

public sealed class KafkaRequestEventArgs : EventArgs
{
    public required KafkaRequest Request { get; init; }
}

internal sealed partial class KafkaClient : IDisposable
{
    private enum State
    {
        WaitingForSize,
        WaitingForMessage,
    }

    private const int BufferSize = 256;
    private Socket _Client;
    private CancellationToken _StopToken;
    private volatile int _Disposed;
    private byte[] _ReceiveBuffer;
    private KafkaClientStream _ReceiveStream = new();
    private volatile int _Receiving;
    private State _State = State.WaitingForSize;
    private int _MessageSize;

    public KafkaClient(ulong id, Socket client, CancellationToken stopToken = default)
    {
        Id = id;
        _Client = client;
        _ReceiveBuffer = ArrayPool<byte>.Shared.Rent(BufferSize);
        _StopToken = stopToken;
    }

    public ulong Id { get; }

    public bool IsConnected => _Disposed == 0 && _Client.Connected;

    public event EventHandler<KafkaRequestEventArgs>? RequestReceived;

    public event EventHandler? Disconnected;

    public void StartReceiving()
    {
        if (_StopToken.IsCancellationRequested)
            return;

        if (Interlocked.CompareExchange(ref _Receiving, 1, 0) != 0)
        {
            return;
        }

        try
        {
            _Client.BeginReceive(_ReceiveBuffer, 0, _ReceiveBuffer.Length, SocketFlags.None, OnReceiveComplete, null);
        }
        catch
        {
            Disconnected?.Invoke(this, EventArgs.Empty);
            Dispose();
        }
    }

    public void Send(ref PacketData packet)
    {
        if (_Disposed != 0)
            return;

        int sendOffset = 0;
        int bytesSent = 0;
        do
        {
            bytesSent += _Client.Send(packet.Buffer, sendOffset, packet.Size - sendOffset, SocketFlags.None);
            sendOffset += bytesSent;
        } while (bytesSent < packet.Size);
    }

    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _Disposed, 1, 0) != 0)
            return;

        _Client.Dispose();
        _Client = null!;
        ArrayPool<byte>.Shared.Return(_ReceiveBuffer);
        _ReceiveBuffer = null!;
        _ReceiveStream.Dispose();
        _ReceiveStream = null!;
        _StopToken = CancellationToken.None;
    }

    private void OnReceiveComplete(IAsyncResult result)
    {
        int readBytes;
        try
        {
            readBytes = _Client.EndReceive(result);
        }
        catch (SocketException)
        {
            Disconnected?.Invoke(this, EventArgs.Empty);
            Dispose();
            return;
        }
        catch (ObjectDisposedException)
        {
            if (_Disposed == 0)
            {
                Disconnected?.Invoke(this, EventArgs.Empty);
                Dispose();
            }

            return;
        }

        if (_StopToken.IsCancellationRequested || readBytes == 0)
        {
            Disconnected?.Invoke(this, EventArgs.Empty);
            Dispose();
            return;
        }

        _ReceiveStream.Push(_ReceiveBuffer, readBytes);
        _ReceiveBuffer = ArrayPool<byte>.Shared.Rent(BufferSize);

        bool canContinue;
        do
        {
            canContinue = ProcessStream();
        } while (canContinue);

        try
        {
            _Client.BeginReceive(_ReceiveBuffer, 0, _ReceiveBuffer.Length, SocketFlags.None, OnReceiveComplete, null);
        }
        catch
        {
            Disconnected?.Invoke(this, EventArgs.Empty);
            Dispose();
        }
    }

    private bool ProcessStream()
    {
        switch (_State)
        {
            case State.WaitingForSize:
            {
                if (_ReceiveStream.Available >= 4)
                {
                    _MessageSize = _ReceiveStream.ReadInt32BigEndian();
                    _State = State.WaitingForMessage;
                    return true;
                }

                break;
            }

            case State.WaitingForMessage:
            {
                if (_ReceiveStream.Available >= _MessageSize)
                {
                    KafkaRequest request;
                    try
                    {
                        request = ReadRequest(_MessageSize);
                    }
                    catch
                    {
                        Disconnected?.Invoke(this, EventArgs.Empty);
                        Dispose();
                        return false;
                    }

                    try
                    {
                        RequestReceived?.Invoke(this, new() { Request = request });
                    }
                    catch
                    {
                        Disconnected?.Invoke(this, EventArgs.Empty);
                        Dispose();
                    }

                    _State = State.WaitingForSize;
                    _MessageSize = 0;
                    return true;
                }

                break;
            }

            default:
            {
                Disconnected?.Invoke(this, EventArgs.Empty);
                Dispose();
                return false;
            }
        }

        return false;
    }

    private KafkaRequest ReadRequest(int messageSize)
    {
        var availableAtStart = _ReceiveStream.Available;
        var header = ReadRequestHeader();
        var consumedBytes = availableAtStart - _ReceiveStream.Available;
        
        byte[]? body = null;
        if (consumedBytes < messageSize)
        {
            var remainingBytes = messageSize - consumedBytes;
            body = new byte[remainingBytes];
            _ReceiveStream.ReadExactly(body.AsSpan());
        }
        
        return new()
        {
            Header = header,
            Body = body
        };
    }

    private KafkaRequestHeader ReadRequestHeader()
    {
        // TODO: This method is assuming request header v2

        var apiKey = _ReceiveStream.ReadInt16BigEndian();
        var apiVersion = _ReceiveStream.ReadInt16BigEndian();
        var correlationId = _ReceiveStream.ReadInt32BigEndian();
        var clientId = _ReceiveStream.ReadNullableString();
        
        // TODO: This is not a byte field. But it will be non-zero in case there are fields.
        //  And we don't support them currently.
        var taggedFieldsCount = _ReceiveStream.ReadByte();
        if (taggedFieldsCount > 0)
        {
            Debug.Assert(false, "Tagged fields are not supported");
        }

        return new()
        {
            ApiKey = apiKey,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId
        };
    }
}