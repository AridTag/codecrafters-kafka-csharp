namespace kafka;

public interface IApiEndPoint
{
    short ApiKey { get; }
    
    short MinimumVersion { get; }
    short MaximumVersion { get; }
    
    PacketData HandleRequest(KafkaRequest request);
}