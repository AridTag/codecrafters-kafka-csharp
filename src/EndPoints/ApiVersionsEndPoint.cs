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

        short errorCode;
        if (request.Header.ApiVersion < MinimumVersion || request.Header.ApiVersion > MaximumVersion)
            errorCode = 35;
        else
            errorCode = 0;

        builder.WriteInt16BigEndian(errorCode);
        if (errorCode != 0)
            return builder.Build();
        
        builder.WriteVariableUInt32((uint)_EndPoints.Value.Count() + 1); // Num end points
        foreach (var endPoint in _EndPoints.Value.OrderBy(e => e.ApiKey))
        {
            builder.WriteInt16BigEndian(endPoint.ApiKey);
            builder.WriteInt16BigEndian(endPoint.MinimumVersion);
            builder.WriteInt16BigEndian(endPoint.MaximumVersion);
            builder.WriteByte(0); // No tagged fields
        }

        builder.WriteInt32BigEndian(0); // Throttle time
        builder.WriteByte(0); // Num tagged fields

        return builder.Build();
    }
}