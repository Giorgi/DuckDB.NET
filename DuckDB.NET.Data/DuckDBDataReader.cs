using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Numerics;

namespace DuckDB.NET.Data;

public class DuckDBDataReader : DbDataReader
{
    private readonly DuckDbCommand command;
    private readonly CommandBehavior behavior;

    private DuckDBResult currentResult;
    private DuckDBDataChunk currentChunk;
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

    private VectorDataReader[] vectorReaders;

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
            currentChunk?.Dispose();
            currentChunk = NativeMethods.Types.DuckDBResultGetChunk(currentResult, currentChunkIndex);
            currentChunkRowCount = (ulong)NativeMethods.DataChunks.DuckDBDataChunkGetSize(currentChunk);
            
            vectorReaders = new VectorDataReader[fieldCount];

            for (int i = 0; i < fieldCount; i++)
            {
                var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(currentChunk, i);
                
                var vectorData = NativeMethods.DataChunks.DuckDBVectorGetData(vector);
                var vectorValidityMask = NativeMethods.DataChunks.DuckDBVectorGetValidity(vector);

                vectorReaders[i] = new VectorDataReader(vector, vectorData, vectorValidityMask, NativeMethods.Query.DuckDBColumnType(ref currentResult, i));
            }
        }
    }

    private T GetFieldData<T>(int ordinal) where T : unmanaged
    {
        return vectorReaders[ordinal].GetFieldData<T>(rowsReadFromCurrentChunk - 1);
    }

    public override bool GetBoolean(int ordinal)
    {
        return GetByte(ordinal) != 0;
    }

    public override byte GetByte(int ordinal)
    {
        return GetFieldData<byte>(ordinal);
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override char GetChar(int ordinal)
    {
        throw new NotSupportedException();
    }

    public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
    {
        throw new NotSupportedException();
    }

    public override string GetDataTypeName(int ordinal)
    {
        return NativeMethods.Query.DuckDBColumnType(ref currentResult, ordinal).ToString();
    }

    public override DateTime GetDateTime(int ordinal)
    {
        return vectorReaders[ordinal].GetDateTime(rowsReadFromCurrentChunk - 1);
    }

    public override decimal GetDecimal(int ordinal)
    {
        return vectorReaders[ordinal].GetDecimal(rowsReadFromCurrentChunk - 1);
    }

    public override double GetDouble(int ordinal)
    {
        return GetFieldData<double>(ordinal);
    }

    public override Type GetFieldType(int ordinal)
    {
        return vectorReaders[ordinal].ColumnType switch
        {
            DuckDBType.Invalid => throw new DuckDBException("Invalid type"),
            DuckDBType.Boolean => typeof(bool),
            DuckDBType.TinyInt => typeof(sbyte),
            DuckDBType.SmallInt => typeof(short),
            DuckDBType.Integer => typeof(int),
            DuckDBType.BigInt => typeof(long),
            DuckDBType.UnsignedTinyInt => typeof(byte),
            DuckDBType.UnsignedSmallInt => typeof(ushort),
            DuckDBType.UnsignedInteger => typeof(uint),
            DuckDBType.UnsignedBigInt => typeof(ulong),
            DuckDBType.Float => typeof(float),
            DuckDBType.Double => typeof(double),
            DuckDBType.Timestamp => typeof(DateTime),
            DuckDBType.Interval => typeof(DuckDBInterval),
            DuckDBType.Date => typeof(DuckDBDateOnly),
            DuckDBType.Time => typeof(DuckDBTimeOnly),
            DuckDBType.HugeInt => typeof(BigInteger),
            DuckDBType.Varchar => typeof(string),
            DuckDBType.Decimal => typeof(decimal),
            DuckDBType.Blob => typeof(Stream),
            DuckDBType.Enum => typeof(Enum),
            DuckDBType.List => typeof(List<>),
            var type => throw new ArgumentException($"Unrecognised type {type} ({(int)type}) in column {ordinal + 1}")
        };
    }

    public override float GetFloat(int ordinal)
    {
        return GetFieldData<float>(ordinal);
    }

    public override Guid GetGuid(int ordinal)
    {
        return new Guid(GetString(ordinal));
    }

    public override short GetInt16(int ordinal)
    {
        return GetFieldData<short>(ordinal);
    }

    public override int GetInt32(int ordinal)
    {
        return GetFieldData<int>(ordinal);
    }

    public override long GetInt64(int ordinal)
    {
        return GetFieldData<long>(ordinal);
    }

    public override string GetName(int ordinal)
    {
        return NativeMethods.Query.DuckDBColumnName(ref currentResult, ordinal).ToManagedString(false);
    }

    public override int GetOrdinal(string name)
    {
        for (var i = 0; i < fieldCount; i++)
        {
            var columnName = NativeMethods.Query.DuckDBColumnName(ref currentResult, i).ToManagedString(false);
            if (name == columnName)
            {
                return i;
            }
        }

        throw new DuckDBException($"Column with name {name} was not found.");
    }

    public override string GetString(int ordinal)
    {
        return vectorReaders[ordinal].GetString(rowsReadFromCurrentChunk - 1);
    }

    public override T GetFieldValue<T>(int ordinal)
    {
        var type = NativeMethods.Query.DuckDBColumnType(ref currentResult, ordinal);

        return type switch
        {
            DuckDBType.List => (T)vectorReaders[ordinal].GetList(rowsReadFromCurrentChunk - 1, typeof(T)),
            DuckDBType.Enum => vectorReaders[ordinal].GetEnum<T>(rowsReadFromCurrentChunk - 1),
            _ => (T)vectorReaders[ordinal].GetValue(rowsReadFromCurrentChunk - 1)
        };
    }

    public override object GetValue(int ordinal)
    {
        return IsDBNull(ordinal) ? DBNull.Value : vectorReaders[ordinal].GetValue(rowsReadFromCurrentChunk - 1);
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
        return vectorReaders[ordinal].GetStream(rowsReadFromCurrentChunk - 1);
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