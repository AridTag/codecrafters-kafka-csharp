using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using kafka.EndPoints;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace kafka;

internal class Program
{
    public static async Task Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        builder.Services.AddHostedService<KafkaServerHost>();
        builder.Services.AddTransient(typeof(Lazy<>), typeof(LazilyResolved<>));
        builder.Services.AddSingleton<IApiEndPoint, ApiVersionsEndPoint>();
        await builder.Build().RunAsync();
    }

    private sealed class LazilyResolved<T> : Lazy<T> where T : notnull
    {
        public LazilyResolved(IServiceProvider serviceProvider)
            : base(serviceProvider.GetRequiredService<T>)
        {
        }
    }
}

internal sealed class KafkaServerHost : IHostedService
{
    private readonly KafkaServer _Server;
    
    public KafkaServerHost(IEnumerable<IApiEndPoint> endPoints)
    {
        _Server = new KafkaServer(endPoints);
    }
    
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _Server.Start();
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return _Server.StopAsync();
    }
}