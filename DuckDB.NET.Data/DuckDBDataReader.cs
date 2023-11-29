using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using DuckDB.NET.Data.Internal.Reader;

namespace DuckDB.NET.Data;

public class DuckDBDataReader : DbDataReader
{
    private readonly DuckDbCommand command;
    private readonly CommandBehavior behavior;

    private DuckDBResult currentResult;
    private DuckDBDataChunk? currentChunk;
    private readonly List<DuckDBResult> queryResults;

    private bool closed;
    private long rowCount;
    private int currentRow;
    private int currentResultIndex;

    private int fieldCount;
    private int recordsAffected = -1;


    private long chunkCount;
    private int currentChunkIndex;
    private ulong rowsReadFromCurrentChunk;
    private ulong currentChunkRowCount;

    private VectorDataReaderBase[] vectorReaders = Array.Empty<VectorDataReaderBase>();

    internal DuckDBDataReader(DuckDbCommand command, List<DuckDBResult> queryResults, CommandBehavior behavior)
    {
        this.command = command;
        this.behavior = behavior;
        this.queryResults = queryResults;

        currentResult = queryResults[0];
        InitReaderData();
    }

    private void InitReaderData()
    {
        currentRow = -1;

        rowCount = NativeMethods.Query.DuckDBRowCount(ref currentResult);
        fieldCount = (int)NativeMethods.Query.DuckDBColumnCount(ref currentResult);
        chunkCount = NativeMethods.Types.DuckDBResultChunkCount(currentResult);

        currentChunkIndex = 0;
        rowsReadFromCurrentChunk = 0;

        InitChunkData();

        //recordsAffected = (int)NativeMethods.Query.DuckDBRowsChanged(currentResult);
    }

    private void InitChunkData()
    {
        unsafe
        {
            foreach (var reader in vectorReaders)
            {
                reader?.Dispose();
            }

            currentChunk?.Dispose();
            currentChunk = NativeMethods.Types.DuckDBResultGetChunk(currentResult, currentChunkIndex);
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
        return vectorReaders[ordinal].GetValue<T>(rowsReadFromCurrentChunk - 1);
    }

    public override object GetValue(int ordinal)
    {
        return IsDBNull(ordinal) ? DBNull.Value : vectorReaders[ordinal].GetValue(rowsReadFromCurrentChunk - 1) ?? DBNull.Value;
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
        return !vectorReaders[ordinal].IsValid(rowsReadFromCurrentChunk - 1);
    }

    public override int FieldCount => fieldCount;

    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => GetValue(GetOrdinal(name));

    public override int RecordsAffected => recordsAffected;

    public override bool HasRows => rowCount > 0;

    public override bool IsClosed => closed;

    public override bool NextResult()
    {
        currentResultIndex++;

        if (currentResultIndex < queryResults.Count)
        {
            currentResult = queryResults[currentResultIndex];

            InitReaderData();
            return true;
        }

        return false;
    }

    public override bool Read()
    {
        var hasMoreRows = ++currentRow < rowCount;

        if (!hasMoreRows) return false;

        if (rowsReadFromCurrentChunk == currentChunkRowCount)
        {
            currentChunkIndex++;
            rowsReadFromCurrentChunk = 0;

            InitChunkData();
        }

        rowsReadFromCurrentChunk++;

        return true;
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
        foreach (var result in queryResults)
        {
            result.Dispose();
        }

        if (behavior == CommandBehavior.CloseConnection)
        {
            command.CloseConnection();
        }

        closed = true;
    }
}