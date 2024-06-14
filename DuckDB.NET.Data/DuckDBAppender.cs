using DuckDB.NET.Data.Internal;
using DuckDB.NET.Data.Internal.Writer;
using DuckDB.NET.Native;
using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace DuckDB.NET.Data;

public class DuckDBAppender : IDisposable
{
    private static readonly ulong DuckDBVectorSize = DuckDBGlobalData.VectorSize;

    private bool closed;
    private readonly Native.DuckDBAppender nativeAppender;
    private readonly string qualifiedTableName;

    private ulong rowCount;

    private readonly DuckDBLogicalType[] logicalTypes;
    private readonly DuckDBDataChunk dataChunk;
    private readonly VectorDataWriterBase[] vectorWriters;

    internal unsafe DuckDBAppender(Native.DuckDBAppender appender, string qualifiedTableName)
    {
        nativeAppender = appender;
        this.qualifiedTableName = qualifiedTableName;

        var columnCount = NativeMethods.Appender.DuckDBAppenderColumnCount(nativeAppender);

        vectorWriters = new VectorDataWriterBase[columnCount];
        logicalTypes = new DuckDBLogicalType[columnCount];
        var logicalTypeHandles = new IntPtr[columnCount];

        for (ulong index = 0; index < columnCount; index++)
        {
            logicalTypes[index] = NativeMethods.Appender.DuckDBAppenderColumnType(nativeAppender, index);
            logicalTypeHandles[index] = logicalTypes[index].DangerousGetHandle();
        }

        dataChunk = NativeMethods.DataChunks.DuckDBCreateDataChunk(logicalTypeHandles, columnCount);

        for (long index = 0; index < vectorWriters.LongLength; index++)
        {
            var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(dataChunk, index);

            vectorWriters[index] = VectorDataWriterFactory.CreateWriter(vector, logicalTypes[index]);
        }
    }

    public DuckDBAppenderRow CreateRow()
    {
        if (closed)
        {
            throw new InvalidOperationException("Appender is already closed");
        }

        if (rowCount == DuckDBVectorSize)
        {
            AppendDataChunk();

            rowCount = 0;
        }

        rowCount++;
        return new DuckDBAppenderRow(qualifiedTableName, vectorWriters, rowCount - 1);
    }

    public void Close()
    {
        closed = true;

        try
        {
            AppendDataChunk();

            foreach (var logicalType in logicalTypes)
            {
                logicalType.Dispose();
            }

            var state = NativeMethods.Appender.DuckDBAppenderClose(nativeAppender);
            if (!state.IsSuccess())
            {
                ThrowLastError(nativeAppender);
            }

            dataChunk.Dispose();
        }
        finally
        {
            nativeAppender.Close();
        }
    }

    public void Dispose()
    {
        if (!closed)
        {
            Close();
        }
    }

    private void AppendDataChunk()
    {
        NativeMethods.DataChunks.DuckDBDataChunkSetSize(dataChunk, rowCount);
        var state = NativeMethods.Appender.DuckDBAppendDataChunk(nativeAppender, dataChunk);

        if (!state.IsSuccess())
        {
            ThrowLastError(nativeAppender);
        }

        NativeMethods.DataChunks.DuckDBDataChunkReset(dataChunk);
    }

    [DoesNotReturn]
    [StackTraceHidden]
    internal static void ThrowLastError(Native.DuckDBAppender appender)
    {
        var errorMessage = NativeMethods.Appender.DuckDBAppenderError(appender).ToManagedString(false);

        throw new DuckDBException(errorMessage);
    }
}
