using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using DuckDB.NET.Data.Internal.Reader;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data;

public class DuckDBDataReader : DbDataReader
{
    private readonly DuckDBCommand command;
    private readonly CommandBehavior behavior;

    private DuckDBResult currentResult;
    private DuckDBDataChunk? currentChunk;

    private int fieldCount;

    private ulong currentChunkRowCount;
    private ulong rowsReadFromCurrentChunk;

    private bool closed;
    private bool hasRows;
    private bool streamingResult;
    private long currentChunkIndex;

    private readonly IEnumerator<DuckDBResult> resultEnumerator;
    private VectorDataReaderBase[] vectorReaders = Array.Empty<VectorDataReaderBase>();

    internal DuckDBDataReader(DuckDBCommand command, IEnumerable<DuckDBResult> queryResults, CommandBehavior behavior)
    {
        this.command = command;
        this.behavior = behavior;
        resultEnumerator = queryResults.GetEnumerator();

        InitNextReader();
    }

    private bool InitNextReader()
    {
        while (resultEnumerator.MoveNext())
        {
            if (NativeMethods.Query.DuckDBResultReturnType(resultEnumerator.Current) == DuckDBResultType.QueryResult)
            {
                currentChunkIndex = 0;
                currentResult = resultEnumerator.Current;

                fieldCount = (int)NativeMethods.Query.DuckDBColumnCount(ref currentResult);
                streamingResult = NativeMethods.Types.DuckDBResultIsStreaming(currentResult) > 0;

                hasRows = InitChunkData();

                return true;
            }
        }

        return false;
    }

    private bool InitChunkData()
    {
        unsafe
        {
            foreach (var reader in vectorReaders)
            {
                reader.Dispose();
            }

            currentChunk?.Dispose();
            currentChunk = streamingResult ? NativeMethods.StreamingResult.DuckDBStreamFetchChunk(currentResult) : NativeMethods.Types.DuckDBResultGetChunk(currentResult, currentChunkIndex);

            rowsReadFromCurrentChunk = 0;

            currentChunkRowCount = (ulong)NativeMethods.DataChunks.DuckDBDataChunkGetSize(currentChunk);

            vectorReaders = new VectorDataReaderBase[fieldCount];

            for (int i = 0; i < fieldCount; i++)
            {
                var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(currentChunk, i);

                var vectorData = NativeMethods.DataChunks.DuckDBVectorGetData(vector);
                var vectorValidityMask = NativeMethods.DataChunks.DuckDBVectorGetValidity(vector);

                vectorReaders[i] = VectorDataReaderFactory.CreateReader(vector, vectorData, vectorValidityMask,
                                                                        NativeMethods.Query.DuckDBColumnType(ref currentResult, i),
                                                                        NativeMethods.Query.DuckDBColumnName(ref currentResult, i).ToManagedString(false));
            }

            return currentChunkRowCount > 0;
        }
    }

    public override bool GetBoolean(int ordinal)
    {
        return GetFieldValue<bool>(ordinal);
    }

    public override byte GetByte(int ordinal)
    {
        return GetFieldValue<byte>(ordinal);
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override char GetChar(int ordinal)
    {
        throw new NotSupportedException();
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        throw new NotSupportedException();
    }

    public override string GetDataTypeName(int ordinal)
    {
        return vectorReaders[ordinal].DuckDBType.ToString();
    }

    public override DateTime GetDateTime(int ordinal)
    {
        return GetFieldValue<DateTime>(ordinal);
    }

    public override decimal GetDecimal(int ordinal)
    {
        return GetFieldValue<decimal>(ordinal);
    }

    public override double GetDouble(int ordinal)
    {
        return GetFieldValue<double>(ordinal);
    }

    public override Type GetFieldType(int ordinal)
    {
        return vectorReaders[ordinal].ClrType;
    }

    public override Type GetProviderSpecificFieldType(int ordinal)
    {
        return vectorReaders[ordinal].ProviderSpecificClrType;
    }

    public override float GetFloat(int ordinal)
    {
        return GetFieldValue<float>(ordinal);
    }

    public override Guid GetGuid(int ordinal)
    {
        return GetFieldValue<Guid>(ordinal);
    }

    public override short GetInt16(int ordinal)
    {
        return GetFieldValue<short>(ordinal);
    }

    public override int GetInt32(int ordinal)
    {
        return GetFieldValue<int>(ordinal);
    }

    public override long GetInt64(int ordinal)
    {
        return GetFieldValue<long>(ordinal);
    }

    public override string GetName(int ordinal)
    {
        return vectorReaders[ordinal].ColumnName;
    }

    public override int GetOrdinal(string name)
    {
        for (var i = 0; i < fieldCount; i++)
        {
            if (GetName(i) == name)
            {
                return i;
            }
        }

        throw new DuckDBException($"Column with name {name} was not found.");
    }

    public override string GetString(int ordinal)
    {
        return GetFieldValue<string>(ordinal);
    }

    public override T GetFieldValue<T>(int ordinal)
    {
        CheckRowRead();

        return vectorReaders[ordinal].GetValue<T>(rowsReadFromCurrentChunk - 1);
    }

    public override object GetValue(int ordinal)
    {
        CheckRowRead();

        return IsDBNull(ordinal) ? DBNull.Value : vectorReaders[ordinal].GetValue(rowsReadFromCurrentChunk - 1);
    }

    public override object GetProviderSpecificValue(int ordinal)
    {
        CheckRowRead();

        return IsDBNull(ordinal) ? DBNull.Value : vectorReaders[ordinal].GetProviderSpecificValue(rowsReadFromCurrentChunk - 1);
    }

    public override int GetValues(object[] values)
    {
        for (var i = 0; i < FieldCount; i++)
        {
            values[i] = GetValue(i);
        }

        return FieldCount;
    }

    public override Stream GetStream(int ordinal)
    {
        return GetFieldValue<Stream>(ordinal);
    }

    public override bool IsDBNull(int ordinal)
    {
        CheckRowRead();

        return !vectorReaders[ordinal].IsValid(rowsReadFromCurrentChunk - 1);
    }

    public override int FieldCount => fieldCount;

    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => GetValue(GetOrdinal(name));

    public override int RecordsAffected => -1;

    public override bool HasRows => hasRows;

    public override bool IsClosed => closed;

    public override bool NextResult()
    {
        return InitNextReader();
    }

    public override bool Read()
    {
        if (rowsReadFromCurrentChunk == currentChunkRowCount)
        {
            currentChunkIndex++;
            var hasData = InitChunkData();

            if (hasData)
            {
                rowsReadFromCurrentChunk++;
            }

            return hasData;
        }
        else
        {
            rowsReadFromCurrentChunk++;

            return true;
        }
    }

    public override int Depth { get; }

    public override IEnumerator GetEnumerator()
    {
        return new DbEnumerator(this, behavior == CommandBehavior.CloseConnection);
    }

    public override DataTable GetSchemaTable()
    {
        var table = new DataTable
        {
            Columns =
            {
                { "ColumnOrdinal", typeof(int) },
                { "ColumnName", typeof(string) },
                { "DataType", typeof(Type) },
                { "ColumnSize", typeof(int) },
                { "AllowDBNull", typeof(bool) }
            }
        };

        var rowData = new object[5];

        for (var i = 0; i < FieldCount; i++)
        {
            rowData[0] = i;
            rowData[1] = GetName(i);
            rowData[2] = GetFieldType(i);
            rowData[3] = -1;
            rowData[4] = true;
            table.Rows.Add(rowData);
        }

        return table;
    }

    public override void Close()
    {
        if (closed) return;

        foreach (var reader in vectorReaders)
        {
            reader.Dispose();
        }

        currentChunk?.Dispose();

        if (behavior == CommandBehavior.CloseConnection)
        {
            command.CloseConnection();
        }

        closed = true;
        resultEnumerator.Dispose();
    }

    private void CheckRowRead()
    {
        if (rowsReadFromCurrentChunk <= 0)
        {
            throw new InvalidOperationException("No row has been read");
        }
    }
}