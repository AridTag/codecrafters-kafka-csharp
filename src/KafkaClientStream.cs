using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace kafka;

internal sealed partial class KafkaClient
{
    private sealed class KafkaClientStream : IDisposable
    {
        private class Buffer
        {
            public required byte[] Data;
            public required int Size;
            public int ReadOffset = 0;

            public Span<byte> SpanAtReadOffset => Data.AsSpan(ReadOffset);
        }

        private readonly List<Buffer> _Buffers = [];
        private Buffer? _CurrentBuffer;

        private int _TotalSize;

        public int Available => _TotalSize - (_CurrentBuffer?.ReadOffset ?? 0);

        /// <summary>
        /// Adds a buffer to the internal collection for processing.
        /// </summary>
        /// <param name="buffer">
        /// The byte array containing the data to be added to the internal collection.
        /// </param>
        /// <param name="size">
        /// The size of the buffer in bytes, specifying the amount of valid data within the buffer.
        /// </param>
        public void Push(byte[] buffer, int size)
        {
            var newBuffer = new Buffer
            {
                Data = buffer,
                Size = size
            };

            _TotalSize += size;

            if (_CurrentBuffer is null)
                _CurrentBuffer = newBuffer;
            else
                _Buffers.Add(newBuffer);

        }

        /// <summary>
        /// Reads a 32-bit integer from the beginning of the internal buffer in big-endian byte order.
        /// </summary>
        /// <returns>
        /// The 32-bit integer value read from the buffer.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if there is not enough data available in the buffer to perform the read operation.
        /// </exception>
        public int ReadInt32BigEndian()
        {
            if (_CurrentBuffer is null || Available < 4)
            {
                throw new InvalidOperationException("Not enough data to read");
            }

            if (_CurrentBuffer.ReadOffset + 4 <= _CurrentBuffer.Size)
            {
                var value = BinaryPrimitives.ReadInt32BigEndian(_CurrentBuffer.SpanAtReadOffset);
                _CurrentBuffer.ReadOffset += 4;
                CheckMoveNextBuffer();
                return value;
            }

            Span<byte> stackSpan = stackalloc byte[4];
            var currentBufferSpan = _CurrentBuffer.SpanAtReadOffset;
            currentBufferSpan.CopyTo(stackSpan);

            var bytesNeededFromNextBuffer = 4 - currentBufferSpan.Length;
            var nextBuffer = _Buffers[0];

            nextBuffer.Data.AsSpan(0, bytesNeededFromNextBuffer)
                .CopyTo(stackSpan[currentBufferSpan.Length..]);

            nextBuffer.ReadOffset += bytesNeededFromNextBuffer;

            CheckMoveNextBuffer();

            return BinaryPrimitives.ReadInt32BigEndian(stackSpan);
        }

        /// <summary>
        /// Reads a 16-bit integer from the current buffer in big-endian format.
        /// </summary>
        /// <returns>
        /// The 16-bit integer value read from the buffer.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if there is not enough data available in the buffer to perform the read operation.
        /// </exception>
        public short ReadInt16BigEndian()
        {
            if (_CurrentBuffer is null || Available < 2)
            {
                throw new InvalidOperationException("Not enough data to read");
            }
            
            if (_CurrentBuffer.ReadOffset + 2 <= _CurrentBuffer.Size)
            {
                var value = BinaryPrimitives.ReadInt16BigEndian(_CurrentBuffer.SpanAtReadOffset);
                _CurrentBuffer.ReadOffset += 2;
                CheckMoveNextBuffer();
                return value;
            }
            
            Span<byte> stackSpan = stackalloc byte[2];
            var currentBufferSpan = _CurrentBuffer.SpanAtReadOffset;
            currentBufferSpan.CopyTo(stackSpan);

            var bytesNeededFromNextBuffer = 2 - currentBufferSpan.Length;
            var nextBuffer = _Buffers[0];

            nextBuffer.Data.AsSpan(0, bytesNeededFromNextBuffer)
                .CopyTo(stackSpan[currentBufferSpan.Length..]);

            nextBuffer.ReadOffset += bytesNeededFromNextBuffer;

            CheckMoveNextBuffer();

            return BinaryPrimitives.ReadInt16BigEndian(stackSpan);
        }

        public byte ReadByte()
        {
            if (_CurrentBuffer is null || Available < 1)
            {
                throw new InvalidOperationException("Not enough data to read");
            }
            
            var value = _CurrentBuffer.SpanAtReadOffset[0];
            _CurrentBuffer.ReadOffset += 1;
            CheckMoveNextBuffer();
            return value;
        }

        public ulong ReadUnsignedVarInt()
        {
            ulong result = 0;
            int shift = 0;
            while (true)
            {
                var b = ReadByte();
                result |= (ulong)(b & 0x7F) << shift;
                if ((b & 0x80) == 0)
                {
                    break;
                }
                shift += 7;
            }

            result = BinaryPrimitives.ReverseEndianness(result);

            return result;
        }

        public string ReadCompactString()
        {
            var length = ReadUnsignedVarInt() - 1;
            if (length > int.MaxValue)
                throw new InvalidOperationException("String length is too long");

            if (length == 0)
                return string.Empty;

            var bytes = ArrayPool<byte>.Shared.Rent((int)length);
            // TODO: This is very much not optimal but im lazy
            for (int i = 0; i < (int)length; ++i)
            {
                bytes[i] = ReadByte();
            }
            
            var value = Encoding.UTF8.GetString(bytes.AsSpan(0, (int)length));
            ArrayPool<byte>.Shared.Return(bytes);
            return value;
        }

        public string? ReadNullableString()
        {
            var length = ReadInt16BigEndian();
            if (length == -1)
                return null;
            
            if (length == 0)
                return string.Empty;
            
            var bytes = ArrayPool<byte>.Shared.Rent(length);
            // TODO: This is very much not optimal but im lazy
            for (int i = 0; i < length; ++i)
            {
                bytes[i] = ReadByte();
            }
            
            var value = Encoding.UTF8.GetString(bytes.AsSpan(0, length));
            ArrayPool<byte>.Shared.Return(bytes);
            return value;
        }

        public void ReadExactly(Span<byte> buffer)
        {
            // TODO: This is very much not optimal but i'm lazy
            for (int i = 0; i < buffer.Length; ++i)
            {
                buffer[i] = ReadByte();
            }
        }

        /// <summary>
        /// Releases the resources used by the current instance of the class.
        /// </summary>
        /// <remarks>
        /// This method ensures that any resources associated with the instance, such as allocated buffers,
        /// are properly released to avoid memory leaks. After calling this method, the instance should no longer be used.
        /// </remarks>
        public void Dispose()
        {
            if (_CurrentBuffer is not null)
            {
                ArrayPool<byte>.Shared.Return(_CurrentBuffer.Data);
                _CurrentBuffer = null;
            }

            foreach (var buffer in _Buffers)
            {
                ArrayPool<byte>.Shared.Return(buffer.Data);
            }

            _Buffers.Clear();
            _TotalSize = 0;
        }

        private void CheckMoveNextBuffer()
        {
            if (_CurrentBuffer is not null)
            {
                if (_CurrentBuffer.ReadOffset == _CurrentBuffer.Size)
                {
                    _TotalSize -= _CurrentBuffer.Size;
                    ArrayPool<byte>.Shared.Return(_CurrentBuffer.Data);
                    _CurrentBuffer = null;
                }
            }

            if (_CurrentBuffer is null && _Buffers.Count > 0)
            {
                _CurrentBuffer = _Buffers[0];
                _Buffers.RemoveAt(0);
            }
        }
    }
}