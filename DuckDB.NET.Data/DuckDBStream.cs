using System;
using System.IO;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data;

class DuckDBStream : Stream
{
    private readonly IntPtr data;

    private long position;

    public DuckDBStream(IntPtr data, long length)
    {
        this.data = data;
        Length = length;
    }

    public override void Flush()
    {
        throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        var bytesToRead = (int)Math.Min(count, Length - position);

        if (bytesToRead > 0)
        {
            unchecked
            {
                var source = position <= int.MaxValue ? IntPtr.Add(data, (int)position) : new IntPtr(data.ToInt64() + position);

                Marshal.Copy(source, buffer, offset, bytesToRead);

                position += bytesToRead;
            }
        }
        return bytesToRead;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        var startingPoint = origin switch
        {
            SeekOrigin.Current => position,
            SeekOrigin.End => Length,
            SeekOrigin.Begin => 0,
            _ => throw new ArgumentOutOfRangeException(nameof(origin), origin, null)
        };

        var newPosition = startingPoint + offset;

        if (newPosition < 0 || newPosition > Length)
        {
            throw new InvalidOperationException("Cannot seek outside of stream");
        }

        position = newPosition;

        return Position;
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;

    public override long Length { get; }

    public override long Position
    {
        get => position;
        set => Seek(value, SeekOrigin.Begin);
    }

    public override void Close()
    {
    }
}