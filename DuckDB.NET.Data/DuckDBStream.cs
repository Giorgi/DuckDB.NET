using System;
using System.IO;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data;

class DuckDBStream : Stream
{
    private long position;
    private readonly DuckDBBlob blob;

    public DuckDBStream(DuckDBBlob blob)
    {
        this.blob = blob;
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
                var source = position <= int.MaxValue ? IntPtr.Add(blob.Data, (int)position) : new IntPtr(blob.Data.ToInt64() + position);

                Marshal.Copy(source, buffer, offset, bytesToRead);

                position += bytesToRead;
            }
        }
        return bytesToRead;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        if (offset == 0)
        {
            position = origin switch
            {
                SeekOrigin.Begin => 0,
                SeekOrigin.End => Length,
                _ => position
            };

            return Position;
        }

        var startingPoint = origin switch
        {
            SeekOrigin.Current => position,
            SeekOrigin.End => Length,
            SeekOrigin.Begin => 0,
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

    public override long Length => blob.Size;

    public override long Position
    {
        get => position;
        set => Seek(value, SeekOrigin.Begin);
    }

    public override void Close()
    {
        blob.Dispose();
    }
}