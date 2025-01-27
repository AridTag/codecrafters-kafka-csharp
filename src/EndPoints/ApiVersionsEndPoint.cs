using System;
using System.Collections.Generic;
using System.Linq;

namespace kafka.EndPoints;

public sealed class ApiVersionsEndPoint : IApiEndPoint
{
    private readonly Lazy<IEnumerable<IApiEndPoint>> _EndPoints;

    public ApiVersionsEndPoint(Lazy<IEnumerable<IApiEndPoint>> endPoints)
    {
        _EndPoints = endPoints;
    }
    
    public short ApiKey => 18;
    
    public short MinimumVersion => 0;
    
    public short MaximumVersion => 4;
    
    public PacketData HandleRequest(KafkaRequest request)
    {
        var builder = new PacketBuilder();
        builder.WriteInt32BigEndian(request.Header.CorrelationId);
        builder.WriteInt16BigEndian(0); // Error code. 0 = no error

        builder.WriteCompactArray(
            _EndPoints.Value.OrderBy(e => e.ApiKey),
            static (ref PacketBuilder builder, IApiEndPoint endPoint) =>
            {
                builder.WriteInt16BigEndian(endPoint.ApiKey);
                builder.WriteInt16BigEndian(endPoint.MinimumVersion);
                builder.WriteInt16BigEndian(endPoint.MaximumVersion);
                builder.WriteByte(0); // No tagged fields
            }
        );

        builder.WriteInt32BigEndian(0); // Throttle time
        builder.WriteByte(0); // Num tagged fields

        return builder.Build();
    }
}