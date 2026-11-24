using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Test.V2;

// The readers flatten a vector that is not flat before they read it. DuckDB hands query results out flat, so no
// query reaches that path; these tests build a constant vector through V2 and read it the way the reader does.
public unsafe class V2VectorTests
{
    [Fact]
    public void AConstantIntegerVectorIsReadInEveryRow()
    {
        Read(DUCKDB_V2_LOGICAL_TYPE_ID.INTEGER, (factory, error) =>
        {
            _duckdb_v2_value* value;
            NativeMethodsV2.duckdb_v2_value_create_int(factory, 7, &value, error).ThrowIfFailed(ref *error);
            return (IntPtr)value;
        }, reader => reader.GetValue<int>(99).Should().Be(7), reader => reader.GetValue<int>(0).Should().Be(7));
    }

    [Fact]
    public void AConstantVarcharVectorIsReadInEveryRow()
    {
        Read(DUCKDB_V2_LOGICAL_TYPE_ID.VARCHAR, (factory, error) =>
        {
            var text = "a value longer than twelve bytes"u8.ToArray();
            fixed (byte* pointer = text)
            {
                _duckdb_v2_value* value;
                NativeMethodsV2.duckdb_v2_value_create_varchar(factory, (sbyte*)pointer, (ulong)text.Length, &value, error).ThrowIfFailed(ref *error);
                return (IntPtr)value;
            }
        }, reader => reader.GetValue<string>(99).Should().Be("a value longer than twelve bytes"),
           reader => reader.GetValue<string>(0).Should().Be("a value longer than twelve bytes"));
    }

    private delegate IntPtr ValueFactory(_duckdb_v2_factory* factory, _duckdb_v2_error_info** error);

    private static void Read(DUCKDB_V2_LOGICAL_TYPE_ID typeId, ValueFactory createValue, params Action<VectorDataReaderBase>[] checks)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var lease = connection.V2Connection.Acquire();

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_factory* factory;
        NativeMethodsV2.duckdb_v2_connection_get_factory(lease.Pointer, &factory, &error).ThrowIfFailed(ref error);

        _duckdb_v2_logical_type* type;
        NativeMethodsV2.duckdb_v2_factory_create_type_from_id(factory, typeId, null, null, null, 0, &type, &error).ThrowIfFailed(ref error);

        _duckdb_v2_data_chunk* chunk = null;
        var value = (_duckdb_v2_value*)createValue(factory, &error);

        try
        {
            NativeMethodsV2.duckdb_v2_data_chunk_create(factory, &type, 1, &chunk, &error).ThrowIfFailed(ref error);

            _duckdb_v2_vector* vector;
            NativeMethodsV2.duckdb_v2_data_chunk_get_vector(chunk, 0, &vector, &error).ThrowIfFailed(ref error);
            NativeMethodsV2.duckdb_v2_vector_make_constant(vector, value, 100, &error).ThrowIfFailed(ref error);

            DUCKDB_V2_VECTOR_TYPE vectorType;
            NativeMethodsV2.duckdb_v2_vector_get_vector_type(vector, &vectorType, &error).ThrowIfFailed(ref error);
            vectorType.Should().Be(DUCKDB_V2_VECTOR_TYPE.CONSTANT);

            using var reader = VectorDataReaderFactory.CreateReader(type, "c");
            reader.Reset(vector);

            foreach (var check in checks)
            {
                check(reader);
            }
        }
        finally
        {
            NativeMethodsV2.duckdb_v2_value_destroy(&value);
            NativeMethodsV2.duckdb_v2_data_chunk_destroy(&chunk);
            NativeMethodsV2.duckdb_v2_logical_type_destroy(&type);
        }
    }
}
