using System;
using System.Buffers;
using System.Buffers.Binary;

namespace kafka;

public ref struct PacketData
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
    private byte[] _Buffer = ArrayPool<byte>.Shared.Rent(256);
    private int _Offset = 4; // Start with 4 bytes for the size

    private void EnsureCapacity(int size)
    {
        if (_Offset + size <= _Buffer.Length)
            return;
        
        var oldBuffer = _Buffer;
        _Buffer = ArrayPool<byte>.Shared.Rent(_Buffer.Length * 2);
        oldBuffer.CopyTo(_Buffer.AsSpan());
        
        ArrayPool<byte>.Shared.Return(oldBuffer);
    }

    public void WriteInt32BigEndian(int value)
    {
        EnsureCapacity(sizeof(int));
        
        BinaryPrimitives.WriteInt32BigEndian(_Buffer.AsSpan(_Offset), value);

        _Offset += sizeof(int);
    }

    public void WriteInt16BigEndian(short value)
    {
        EnsureCapacity(sizeof(short));
        
        BinaryPrimitives.WriteInt16BigEndian(_Buffer.AsSpan(_Offset), value);

        _Offset += sizeof(short);
    }

    public void WriteByte(byte value)
    {
        EnsureCapacity(1);
        _Buffer[_Offset] = value;
        _Offset += 1;
    }
    
    public void WriteVariableUInt32(uint value)
    {
        while (value > 0)
        {
            byte b = (byte) (value & 0x7F);
            value >>= 7;
            if (value > 0)
                b |= 0x80;
            
            WriteByte(b);
        }
    }

    public PacketData Build()
    {
        var messageSize = _Offset - 4;
        BinaryPrimitives.WriteInt32BigEndian(_Buffer.AsSpan(), messageSize);
        
        return new ()
        {
            Buffer = _Buffer,
            Size = _Offset,
        };
    }
}