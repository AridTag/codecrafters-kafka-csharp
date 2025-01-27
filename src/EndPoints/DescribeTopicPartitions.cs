namespace kafka.EndPoints;

public sealed class DescribeTopicPartitions : IApiEndPoint
{
    public short ApiKey => 75;
    public short MinimumVersion => 0;
    public short MaximumVersion => 0;
    
    public PacketData HandleRequest(KafkaRequest request)
    {
        throw new System.NotImplementedException();
    }
}