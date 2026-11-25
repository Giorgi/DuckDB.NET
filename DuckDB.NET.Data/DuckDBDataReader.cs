using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.Execution;
using DuckDB.NET.Native.V2;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data;

public sealed class DuckDBDataReader : DbDataReader
{
    private readonly DuckDBCommand command;
    private readonly CommandBehavior behavior;

    private DuckDBV2StatementResult? currentResult;
    private DuckDBV2DataChunk? currentChunk;

    private int fieldCount;

    private ulong currentChunkRowCount;
    private ulong rowsReadFromCurrentChunk;

    private bool closed;
    private int recordsAffected = -1;
    private bool hasRows;

    private readonly IEnumerator<DuckDBV2StatementResult> resultEnumerator;
    private VectorDataReaderBase[] vectorReaders = [];
    private Dictionary<string, int> columnMapping = [];

    internal DuckDBDataReader(DuckDBCommand command, IEnumerable<DuckDBV2StatementResult> queryResults, CommandBehavior behavior)
    {
        this.command = command;
        this.behavior = behavior;
        resultEnumerator = queryResults.GetEnumerator();

        // The caller gets no reader when this throws, so release the result and statements here.
        try
        {
            InitNextReader();
        }
        catch
        {
            Close();
            throw;
        }
    }

    private bool InitNextReader()
    {
        // V2 allows one open result per connection, and the next statement runs only once this one is done.
        LeaveCurrentResult();

        while (resultEnumerator.MoveNext())
        {
            var result = resultEnumerator.Current;

            if (result.ReturnsRows)
            {
                currentResult = result;

                try
                {
                    CreateReaders(result);
                    hasRows = InitChunkData();
                }
                catch
                {
                    // The readers for this result were never built, so report no columns rather than index into them.
                    fieldCount = 0;
                    throw;
                }

                return true;
            }

            // A statement that returns no rows runs only when its result is drained.
            using (result)
            {
                AddRecordsAffected(result.StatementType, result.Finish());
            }
        }

        return false;
    }

    private unsafe void CreateReaders(DuckDBV2StatementResult result)
    {
        using var schema = result.GetSchema();

        DisposeReaders();

        fieldCount = schema.Count;
        vectorReaders = new VectorDataReaderBase[fieldCount];
        columnMapping = [];

        for (var index = 0; index < fieldCount; index++)
        {
            var (name, logicalType) = schema.GetField(index);
            vectorReaders[index] = VectorDataReaderFactory.CreateReader((_duckdb_v2_logical_type*)logicalType, name);
            columnMapping.TryAdd(name, index);
        }
    }

    // Done with the current result, read to its end or not: release its chunk, run a statement that can write to
    // its end, and count the rows it affected. The column readers stay until another result replaces them, so the
    // last result's columns are still described after NextResult() returns false (DataTable.Load asks for them).
    // They point into the released chunk, but the zero row counters keep CheckRowRead from reading it.
    private void LeaveCurrentResult()
    {
        rowsReadFromCurrentChunk = 0;
        currentChunkRowCount = 0;

        currentChunk?.Dispose();
        currentChunk = null;

        if (currentResult is { } result)
        {
            currentResult = null;

            using (result)
            {
                AddRecordsAffected(result.StatementType, result.Leave());
            }
        }
    }

    private void DisposeReaders()
    {
        foreach (var reader in vectorReaders)
        {
            reader?.Dispose();
        }

        vectorReaders = [];
        fieldCount = 0;
    }

    private void AddRecordsAffected(DuckDBStatementType statementType, long rows)
    {
        if (DuckDBCommand.CountsRows(statementType))
        {
            recordsAffected = Math.Max(recordsAffected, 0) + (int)rows;
        }
    }

    private unsafe bool InitChunkData()
    {
        // Reset before fetching: if the fetch throws, the readers still point into the chunk freed here,
        // and zero counters make CheckRowRead reject any value access instead of reading that memory.
        rowsReadFromCurrentChunk = 0;
        currentChunkRowCount = 0;

        currentChunk?.Dispose();
        currentChunk = null;

        if (currentResult is null)
        {
            return false;
        }

        // A chunk can be empty without being the last one, so only the end of the result ends the reading.
        while (currentResult.FetchChunk() is { } chunk)
        {
            currentChunk = chunk;
            currentChunkRowCount = DuckDBV2StatementResult.GetSize(chunk);

            if (currentChunkRowCount == 0)
            {
                chunk.Dispose();
                currentChunk = null;
                continue;
            }

            for (var index = 0; index < fieldCount; index++)
            {
                vectorReaders[index].Reset(DuckDBV2Vectors.GetVector(chunk, index));
            }

            return true;
        }

        return false;
    }

    public override bool GetBoolean(int ordinal)
    {
        return GetFieldValue<bool>(ordinal);
    }

    public override byte GetByte(int ordinal)
    {
        return GetFieldValue<byte>(ordinal);
    }

    /// <summary>
    /// IDataRecord.GetBytes: reads "from the specified column offset into the buffer as an array, starting at the given
    /// buffer offset", and "If you pass a buffer that is null, GetBytes returns the length of the row in bytes."
    /// </summary>
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        using var stream = GetStream(ordinal);
        if (buffer is null)
        {
            return stream.Length;
        }

        ValidateBufferArguments(dataOffset, buffer.Length, bufferOffset, length);
        if (dataOffset >= stream.Length)
        {
            return 0;
        }

        stream.Position = dataOffset;
        var count = (int)Math.Min(length, stream.Length - dataOffset);
        var read = 0;
        while (read < count)
        {
            var chunk = stream.Read(buffer, bufferOffset + read, count - read);
            if (chunk == 0)
            {
                break;
            }

            read += chunk;
        }

        return read;
    }

    public override char GetChar(int ordinal)
    {
        throw new NotSupportedException();
    }

    /// <summary>
    /// IDataRecord.GetChars: reads characters "from the specified column offset into the buffer as an array, starting at
    /// the given buffer offset", and "If you pass a buffer that is null, GetChars returns the length of the field in
    /// characters."
    /// </summary>
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        CheckRowRead();

        if (buffer != null)
        {
            ValidateBufferArguments(dataOffset, buffer.Length, bufferOffset, length);
        }

        if (vectorReaders[ordinal] is not StringVectorDataReader reader)
        {
            throw new NotSupportedException($"GetChars is not supported for {vectorReaders[ordinal].DuckDBType} columns");
        }

        return reader.GetChars(rowsReadFromCurrentChunk - 1, dataOffset, buffer, bufferOffset, length);
    }

    private static void ValidateBufferArguments(long dataOffset, int bufferLength, int bufferOffset, int length)
    {
        if (dataOffset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(dataOffset), dataOffset, "The data offset cannot be negative.");
        }

        if (bufferOffset < 0 || bufferOffset > bufferLength)
        {
            throw new ArgumentOutOfRangeException(nameof(bufferOffset), bufferOffset, "The buffer offset must be within the buffer.");
        }

        if (length < 0 || length > bufferLength - bufferOffset)
        {
            throw new ArgumentOutOfRangeException(nameof(length), length, "The length must fit in the buffer after the buffer offset.");
        }
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

    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type GetFieldType(int ordinal)
    {
        return vectorReaders[ordinal].ClrType;
    }

    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
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
        if (columnMapping.TryGetValue(name, out var index))
        {
            return index;
        }

        // IDataRecord.GetOrdinal: "performs a case-sensitive lookup first. If it fails, a second case-insensitive search
        // is made. GetOrdinal is kana-width insensitive. If the index of the named field is not found, an
        // IndexOutOfRangeException is thrown."
        var compareInfo = CultureInfo.InvariantCulture.CompareInfo;
        for (var i = 0; i < vectorReaders.Length; i++)
        {
            if (compareInfo.Compare(vectorReaders[i].ColumnName, name, CompareOptions.IgnoreCase | CompareOptions.IgnoreKanaType | CompareOptions.IgnoreWidth) == 0)
            {
                return i;
            }
        }

        throw new IndexOutOfRangeException($"Column with name {name} was not found.");
    }

    public override string GetString(int ordinal)
    {
        return GetFieldValue<string>(ordinal);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override T GetFieldValue<T>(int ordinal)
    {
        if (typeof(T) == typeof(object))
        {
            return (T)GetValue(ordinal);
        }

        CheckRowRead();

        return vectorReaders[ordinal].GetValueStrict<T>(rowsReadFromCurrentChunk - 1);
    }

    public override object GetValue(int ordinal)
    {
        CheckRowRead();

        var offset = rowsReadFromCurrentChunk - 1;
        var reader = vectorReaders[ordinal];

        return reader.IsValid(offset) ? reader.GetValue(offset) : DBNull.Value;
    }

    public override object GetProviderSpecificValue(int ordinal)
    {
        CheckRowRead();

        var offset = rowsReadFromCurrentChunk - 1;
        var reader = vectorReaders[ordinal];

        return reader.IsValid(offset) ? reader.GetProviderSpecificValue(offset) : DBNull.Value;
    }

    public override int GetValues(object[] values)
    {
        ArgumentNullException.ThrowIfNull(values);
        CheckRowRead();

        var offset = rowsReadFromCurrentChunk - 1;

        // IDataRecord.GetValues: "You can pass an Object array that contains fewer than the number of columns that are
        // contained in the resulting row. Only the amount of data the Object array holds is copied to the array."
        var count = Math.Min(values.Length, FieldCount);

        for (var i = 0; i < count; i++)
        {
            var reader = vectorReaders[i];
            values[i] = reader.IsValid(offset) ? reader.GetValue(offset) : DBNull.Value;
        }

        return count;
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

    /// <summary>
    /// The rows inserted, updated, deleted, or copied by the statements read so far; -1 when none of them reports an
    /// affected-row count. DbDataReader.RecordsAffected: "-1 for SELECT statements; 0 if no rows were affected".
    /// </summary>
    public override int RecordsAffected => recordsAffected;

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
                { SchemaTableColumn.ColumnName, typeof(string) },
                { SchemaTableColumn.ColumnOrdinal, typeof(int) },
                { SchemaTableColumn.ColumnSize, typeof(int) },
                { SchemaTableColumn.NumericPrecision, typeof(byte)},
                { SchemaTableColumn.NumericScale, typeof(byte) },
                { SchemaTableColumn.DataType, typeof(Type) },
                { SchemaTableColumn.AllowDBNull, typeof(bool)  }
            }
        };

        var rowData = new object[7];

        for (var i = 0; i < FieldCount; i++)
        {
            rowData[0] = GetName(i);
            rowData[1] = i;
            rowData[2] = -1;
            rowData[5] = GetFieldType(i);
            rowData[6] = true;

            if (vectorReaders[i] is DecimalVectorDataReader decimalVectorDataReader)
            {
                rowData[4] = decimalVectorDataReader.Scale;
                rowData[3] = decimalVectorDataReader.Precision;
            }
            else
            {
                rowData[3] = rowData[4] = DBNull.Value;
            }

            table.Rows.Add(rowData);
        }

        return table;
    }

    public override void Close()
    {
        if (closed) return;

        try
        {
            // A statement that can write runs to its end here, so what it writes is applied; it can still fail.
            LeaveCurrentResult();
        }
        finally
        {
            // Finish the reader's own cleanup first, so an exception from closing the connection cannot skip it.
            // The readers are disposed but kept, as with V1: DataTable.Load closes the reader and then asks for its
            // schema.
            foreach (var reader in vectorReaders)
            {
                reader?.Dispose();
            }

            closed = true;

            try
            {
                resultEnumerator.Dispose();
            }
            finally
            {
                if (behavior == CommandBehavior.CloseConnection)
                {
                    command.CloseConnection();
                }
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CheckRowRead()
    {
        if (rowsReadFromCurrentChunk <= 0)
        {
            throw NoRowRead();
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static InvalidOperationException NoRowRead()
    {
        return new InvalidOperationException("No row has been read");
    }
}
