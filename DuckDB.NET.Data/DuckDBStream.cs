using System;
using System.IO;
using System.Reflection;
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

        unchecked
        {
            var source = position <= int.MaxValue ? IntPtr.Add(blob.Data, (int)position) : new IntPtr(blob.Data.ToInt64() + position);

            Marshal.Copy(source, buffer, offset, bytesToRead);

            position += bytesToRead;

            return bytesToRead;
        }
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
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
    public override bool CanSeek => false;
    public override bool CanWrite => false;

    public override long Length => blob.Size;

    public override long Position
    {
        get => position;
        set => throw new NotSupportedException();
    }

    public override void Close()
    {
        blob.Dispose();
    }
}