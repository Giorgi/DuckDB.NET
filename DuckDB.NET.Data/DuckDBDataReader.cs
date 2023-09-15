using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Numerics;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data;

public class DuckDBDataReader : DbDataReader
{
    private const int InlineStringMaxLength = 12;
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
    private int recordsAffected;
    private readonly Dictionary<int, IntPtr> vectors = new();
    private readonly Dictionary<int, IntPtr> vectorValidityMask = new();

    private long chunkCount;
    private int currentChunkIndex;
    private int rowsReadFromCurrentChunk;
    private long currentChunkRowCount;

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
        currentChunk?.Dispose();
        currentChunk = NativeMethods.Types.DuckDBResultGetChunk(currentResult, currentChunkIndex);
        currentChunkRowCount = NativeMethods.DataChunks.DuckDBDataChunkGetSize(currentChunk);

        for (int i = 0; i < fieldCount; i++)
        {
            var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(currentChunk, i);

            vectors[i] = NativeMethods.DataChunks.DuckDBVectorGetData(vector);
            vectorValidityMask[i] = NativeMethods.DataChunks.DuckDBVectorGetValidity(vector);
        }
    }

    public override bool GetBoolean(int ordinal)
    {
        var data = vectors[ordinal];
        return Marshal.ReadByte(data, (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<byte>()) != 0;
    }

    public override byte GetByte(int ordinal)
    {
        var data = vectors[ordinal];
        return Marshal.ReadByte(data, (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<byte>());
    }

    private sbyte GetSByte(int ordinal)
    {
        var data = vectors[ordinal];
        return (sbyte)Marshal.ReadByte(data, (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<byte>());
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
        var data = vectors[ordinal];
        var timestampStruct = Marshal.PtrToStructure<DuckDBTimestampStruct>(data + (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<DuckDBTimestampStruct>());

        return timestampStruct.ToDateTime();
    }

    private DuckDBDateOnly GetDateOnly(int ordinal)
    {
        var data = vectors[ordinal];
        var date = Marshal.PtrToStructure<DuckDBDate>(data + (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<DuckDBDate>());
        return NativeMethods.DateTime.DuckDBFromDate(date);
    }

    private DuckDBTimeOnly GetTimeOnly(int ordinal)
    {
        var data = vectors[ordinal];
        var time = Marshal.PtrToStructure<DuckDBTime>(data + (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<DuckDBTime>());
        return NativeMethods.DateTime.DuckDBFromTime(time);
    }

    public override decimal GetDecimal(int ordinal)
    {
        using (var logicalType = NativeMethods.Query.DuckDBColumnLogicalType(ref currentResult, ordinal))
        {
            var scale = NativeMethods.LogicalType.DuckDBDecimalScale(logicalType);
            var internalType = NativeMethods.LogicalType.DuckDBDecimalInternalType(logicalType);

            decimal result = 0;

            var pow = (decimal)Math.Pow(10, scale);

            switch (internalType)
            {
                case DuckDBType.DuckdbTypeSmallInt:
                    result = decimal.Divide(GetInt16(ordinal), pow);
                    break;
                case DuckDBType.DuckdbTypeInteger:
                    result = decimal.Divide(GetInt32(ordinal), pow);
                    break;
                case DuckDBType.DuckdbTypeBigInt:
                    result = decimal.Divide(GetInt64(ordinal), pow);
                    break;
                case DuckDBType.DuckdbTypeHugeInt:
                {
                    var hugeInt = GetBigInteger(ordinal);

                    result = (decimal)BigInteger.DivRem(hugeInt, (BigInteger)pow, out var remainder);

                    result += decimal.Divide((decimal)remainder, pow);
                    break;
                }
            }

            return result;
        }
    }

    public override double GetDouble(int ordinal)
    {
        var data = vectors[ordinal];
        return Marshal.PtrToStructure<double>(data + (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<double>());
    }

    public override Type GetFieldType(int ordinal)
    {
        return NativeMethods.Query.DuckDBColumnType(ref currentResult, ordinal) switch
        {
            DuckDBType.DuckdbTypeInvalid => throw new DuckDBException("Invalid type"),
            DuckDBType.DuckdbTypeBoolean => typeof(bool),
            DuckDBType.DuckdbTypeTinyInt => typeof(sbyte),
            DuckDBType.DuckdbTypeSmallInt => typeof(short),
            DuckDBType.DuckdbTypeInteger => typeof(int),
            DuckDBType.DuckdbTypeBigInt => typeof(long),
            DuckDBType.DuckdbTypeUnsignedTinyInt => typeof(byte),
            DuckDBType.DuckdbTypeUnsignedSmallInt => typeof(ushort),
            DuckDBType.DuckdbTypeUnsignedInteger => typeof(uint),
            DuckDBType.DuckdbTypeUnsignedBigInt => typeof(ulong),
            DuckDBType.DuckdbTypeFloat => typeof(float),
            DuckDBType.DuckdbTypeDouble => typeof(double),
            DuckDBType.DuckdbTypeTimestamp => typeof(DateTime),
            DuckDBType.DuckdbTypeInterval => typeof(DuckDBInterval),
            DuckDBType.DuckdbTypeDate => typeof(DuckDBDateOnly),
            DuckDBType.DuckdbTypeTime => typeof(DuckDBTimeOnly),
            DuckDBType.DuckdbTypeHugeInt => typeof(BigInteger),
            DuckDBType.DuckdbTypeVarchar => typeof(string),
            DuckDBType.DuckdbTypeDecimal => typeof(decimal),
            DuckDBType.DuckdbTypeBlob => typeof(Stream),
            var type => throw new ArgumentException($"Unrecognised type {type} ({(int)type}) in column {ordinal + 1}")
        };
    }

    public override float GetFloat(int ordinal)
    {
        var data = vectors[ordinal];
        return Marshal.PtrToStructure<float>(data + (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<float>());
    }

    public override Guid GetGuid(int ordinal)
    {
        return new Guid(GetString(ordinal));
    }

    public override short GetInt16(int ordinal)
    {
        var data = vectors[ordinal];
        return Marshal.ReadInt16(data, (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<short>());
    }

    public override int GetInt32(int ordinal)
    {
        var data = vectors[ordinal];
        return Marshal.ReadInt32(data, (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<int>());
    }

    public override long GetInt64(int ordinal)
    {
        var data = vectors[ordinal];
        return Marshal.ReadInt64(data, (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<long>());
    }

    private ushort GetUInt16(int ordinal)
    {
        var data = vectors[ordinal];
        return (ushort)Marshal.ReadInt32(data, (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<ushort>());
    }

    private uint GetUInt32(int ordinal)
    {
        var data = vectors[ordinal];
        return (uint)Marshal.ReadInt32(data, (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<uint>());
    }

    private ulong GetUInt64(int ordinal)
    {
        var data = vectors[ordinal];
        return (ulong)Marshal.ReadInt32(data, (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<ulong>());
    }

    private BigInteger GetBigInteger(int ordinal)
    {
        var data = vectors[ordinal];
        var hugeInt = Marshal.PtrToStructure<DuckDBHugeInt>(data + (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<DuckDBHugeInt>());

        return hugeInt.ToBigInteger();
    }

    public override string GetName(int ordinal)
    {
        return NativeMethods.Query.DuckDBColumnName(ref currentResult, ordinal).ToManagedString(false);
    }

    public override int GetOrdinal(string name)
    {
        var columnCount = NativeMethods.Query.DuckDBColumnCount(ref currentResult);
        for (var i = 0; i < columnCount; i++)
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
        var data = vectors[ordinal] + (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<DuckDBString>();

        var length = Marshal.ReadInt32(data);

        if (length <= InlineStringMaxLength)
        {
            return (data + Marshal.SizeOf<int>()).ToManagedString(false, length);
        }
        else
        {
            var intPtr = Marshal.ReadIntPtr(data + Marshal.SizeOf<int>() * 2);
            return intPtr.ToManagedString(false, length);
        }
    }

    public override object GetValue(int ordinal)
    {
        if (IsDBNull(ordinal))
        {
            return DBNull.Value;
        }

        return NativeMethods.Query.DuckDBColumnType(ref currentResult, ordinal) switch
        {
            DuckDBType.DuckdbTypeInvalid => throw new DuckDBException("Invalid type"),
            DuckDBType.DuckdbTypeBoolean => GetBoolean(ordinal),
            DuckDBType.DuckdbTypeTinyInt => GetSByte(ordinal),
            DuckDBType.DuckdbTypeSmallInt => GetInt16(ordinal),
            DuckDBType.DuckdbTypeInteger => GetInt32(ordinal),
            DuckDBType.DuckdbTypeBigInt => GetInt64(ordinal),
            DuckDBType.DuckdbTypeUnsignedTinyInt => GetByte(ordinal),
            DuckDBType.DuckdbTypeUnsignedSmallInt => GetUInt16(ordinal),
            DuckDBType.DuckdbTypeUnsignedInteger => GetUInt32(ordinal),
            DuckDBType.DuckdbTypeUnsignedBigInt => GetUInt64(ordinal),
            DuckDBType.DuckdbTypeFloat => GetFloat(ordinal),
            DuckDBType.DuckdbTypeDouble => GetDouble(ordinal),
            DuckDBType.DuckdbTypeTimestamp => GetDateTime(ordinal),
            DuckDBType.DuckdbTypeInterval => GetDuckDBInterval(ordinal),
            DuckDBType.DuckdbTypeDate => GetDateOnly(ordinal),
            DuckDBType.DuckdbTypeTime => GetTimeOnly(ordinal),
            DuckDBType.DuckdbTypeHugeInt => GetBigInteger(ordinal),
            DuckDBType.DuckdbTypeVarchar => GetString(ordinal),
            DuckDBType.DuckdbTypeDecimal => GetDecimal(ordinal),
            DuckDBType.DuckdbTypeBlob => GetStream(ordinal),
            var type => throw new ArgumentException($"Unrecognised type {type} ({(int)type}) in column {ordinal + 1}")
        };
    }

    private DuckDBInterval GetDuckDBInterval(int ordinal)
    {
        var data = vectors[ordinal];
        var interval = Marshal.PtrToStructure<DuckDBInterval>(data + (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<DuckDBInterval>());
        return interval;
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
        var data = vectors[ordinal] + (rowsReadFromCurrentChunk - 1) * Marshal.SizeOf<DuckDBString>();

        var length = Marshal.ReadInt32(data);

        var blobPointer = length <= InlineStringMaxLength
            ? data + Marshal.SizeOf<int>()
            : Marshal.ReadIntPtr(data + Marshal.SizeOf<int>() * 2);

        return new DuckDBStream(blobPointer, length);
    }

    public override bool IsDBNull(int ordinal)
    {
        var validityMaskEntryIndex = (rowsReadFromCurrentChunk - 1) / 64;
        var validityBitIndex = (rowsReadFromCurrentChunk - 1) % 64;

        var validityMaskEntryPtr = vectorValidityMask[ordinal] + validityMaskEntryIndex * Marshal.SizeOf<ulong>();
        var validityBit = 1ul << validityBitIndex;

        var isValid = (Marshal.PtrToStructure<ulong>(validityMaskEntryPtr) & validityBit) != 0;
        return !isValid;
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