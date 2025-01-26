namespace kafka;

public struct KafkaRequest
{
    public required KafkaRequestHeader Header;
    public byte[]? Body;
}

public struct KafkaRequestHeader
{
    public short ApiKey;
    public short ApiVersion;
    public int CorrelationId;
    
    public string? ClientId; // header v1 adds client id
    
    //public List<TaggedField>? TaggedFields; // header v2 adds tagged fields
}