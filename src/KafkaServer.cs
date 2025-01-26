using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace kafka;

internal sealed class KafkaServer
{
    private readonly IReadOnlyDictionary<short, IApiEndPoint> _EndPoints;
    private Task? _WorkerTask;
    private CancellationTokenSource? _CancellationTokenSource;
    
    private ConcurrentDictionary<ulong, KafkaClient> _Clients = new();

    public KafkaServer(IEnumerable<IApiEndPoint> endPoints)
    {
        _EndPoints = endPoints.ToDictionary(e => e.ApiKey, e => e);
    }

    public void Start()
    {
        if (_WorkerTask is not null)
            return;
        
        _CancellationTokenSource = new ();
        _WorkerTask = RunServerAsync(_CancellationTokenSource.Token);
    }

    public async Task StopAsync()
    {
        if (_WorkerTask is null || _CancellationTokenSource is null)
            return;
        
        await _CancellationTokenSource.CancelAsync().ConfigureAwait(false);
        await _WorkerTask.ConfigureAwait(false);
    }

    private async Task RunServerAsync(CancellationToken stopToken)
    {
        ulong nextClientId = 1;
        using var server = new TcpListener(IPAddress.Any, 9092);
        server.Start();

        while (!stopToken.IsCancellationRequested)
        {
            var newClientId = nextClientId;
            ++nextClientId;

            Socket clientSocket;
            try
            {
                clientSocket = await server.AcceptSocketAsync(stopToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            var newClient = new KafkaClient(newClientId, clientSocket, stopToken);
            if (!_Clients.TryAdd(newClientId, newClient))
            {
                // This shouldn't ever happen, but you know just in case
                newClient.Dispose();
                continue;
            }
            
            newClient.Disconnected += OnClientDisconnected;
            newClient.RequestReceived += OnRequestReceived;
            
            newClient.StartReceiving();
        }
    }

    private void OnRequestReceived(object? sender, KafkaRequestEventArgs e)
    {
        var client = (KafkaClient)sender!;
        PacketData response;
        if (_EndPoints.TryGetValue(e.Request.Header.ApiKey, out var endPoint))
        {
            try
            {
                response = endPoint.HandleRequest(e.Request);
            }
            catch
            {
                // TODO: Do something
                throw;
            }
        }
        else
        {
            response = new PacketBuilder()
                .WriteInt32BigEndian(e.Request.Header.CorrelationId)
                .WriteInt16BigEndian((short)3) // Unknown topic or partition
                .Build();
        }
    
        try
        {
            client.Send(ref response);
        }
        finally
        {
            response.Dispose();
        }
    }

    private void OnClientDisconnected(object? sender, EventArgs e)
    {
        var client = (KafkaClient)sender!;
        _Clients.TryRemove(client.Id, out _);
    }
}