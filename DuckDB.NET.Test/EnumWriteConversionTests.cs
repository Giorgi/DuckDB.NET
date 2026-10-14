using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Test;

// A C# enum appended to an ENUM column is stored as its number: the position of a name in the list of names of the
// ENUM type. These tests pin what is stored for every integer an enum can be based on and which numbers are refused,
// for an enum passed as itself and for one passed as a System.Enum, which is boxed and takes another path. A negative
// value of a signed enum is refused like a number past the last name.
public class EnumWriteConversionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    private enum SByteEnum : sbyte { First, Second, Third, PastTheEnd, Negative = -1, Max = sbyte.MaxValue }
    private enum ByteEnum : byte { First, Second, Third, PastTheEnd, Max = byte.MaxValue }
    private enum Int16Enum : short { First, Second, Third, PastTheEnd, Negative = -1, Max = short.MaxValue }
    private enum UInt16Enum : ushort { First, Second, Third, PastTheEnd, Max = ushort.MaxValue }
    private enum Int32Enum { First, Second, Third, PastTheEnd, Negative = -1, Max = int.MaxValue }
    private enum UInt32Enum : uint { First, Second, Third, PastTheEnd, Max = uint.MaxValue }
    private enum Int64Enum : long { First, Second, Third, PastTheEnd, Negative = -1, Max = long.MaxValue }
    private enum UInt64Enum : ulong { First, Second, Third, PastTheEnd, Max = ulong.MaxValue }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AnEnumIsStoredAsThePositionOfAName(bool boxed)
    {
        StoresThreeNames(boxed, SByteEnum.First, SByteEnum.Second, SByteEnum.Third);
        StoresThreeNames(boxed, ByteEnum.First, ByteEnum.Second, ByteEnum.Third);
        StoresThreeNames(boxed, Int16Enum.First, Int16Enum.Second, Int16Enum.Third);
        StoresThreeNames(boxed, UInt16Enum.First, UInt16Enum.Second, UInt16Enum.Third);
        StoresThreeNames(boxed, Int32Enum.First, Int32Enum.Second, Int32Enum.Third);
        StoresThreeNames(boxed, UInt32Enum.First, UInt32Enum.Second, UInt32Enum.Third);
        StoresThreeNames(boxed, Int64Enum.First, Int64Enum.Second, Int64Enum.Third);
        StoresThreeNames(boxed, UInt64Enum.First, UInt64Enum.Second, UInt64Enum.Third);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ANumberWithoutANameIsRefused(bool boxed)
    {
        IsRefused(boxed, SByteEnum.PastTheEnd, SByteEnum.Negative, SByteEnum.Max);
        IsRefused(boxed, ByteEnum.PastTheEnd, ByteEnum.Max);
        IsRefused(boxed, Int16Enum.PastTheEnd, Int16Enum.Negative, Int16Enum.Max);
        IsRefused(boxed, UInt16Enum.PastTheEnd, UInt16Enum.Max);
        IsRefused(boxed, Int32Enum.PastTheEnd, Int32Enum.Negative, Int32Enum.Max);
        IsRefused(boxed, UInt32Enum.PastTheEnd, UInt32Enum.Max);
        IsRefused(boxed, Int64Enum.PastTheEnd, Int64Enum.Negative, Int64Enum.Max);
        IsRefused(boxed, UInt64Enum.PastTheEnd, UInt64Enum.Max);
    }

    // An enum is written to an ENUM column only. The message names System.Enum, whichever enum it was.
    [Fact]
    public void AnEnumIsRefusedByAColumnThatIsNotAnEnum()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE enumIntoInteger (value INTEGER)");

        Connection.Invoking(c =>
        {
            using var appender = c.CreateAppender("enumIntoInteger");
            appender.CreateRow().AppendValue(Int32Enum.First).EndRow();
        }).Should().Throw<InvalidOperationException>().WithMessage("Cannot write Enum to Integer column");

        Connection.Invoking(c =>
        {
            using var appender = c.CreateAppender("enumIntoInteger");
            appender.CreateRow().AppendValue((Enum)Int32Enum.First).EndRow();
        }).Should().Throw<InvalidOperationException>().WithMessage("Cannot write Enum to Integer column");
    }

    private void StoresThreeNames<TEnum>(bool boxed, params TEnum[] values) where TEnum : struct, Enum
    {
        var table = CreateTable<TEnum>();

        using (var appender = Connection.CreateAppender(table))
        {
            foreach (var value in values)
            {
                Append(appender.CreateRow(), value, boxed).EndRow();
            }
        }

        Command.CommandText = $"SELECT value::VARCHAR FROM {table} ORDER BY rowid";
        using var reader = Command.ExecuteReader();

        foreach (var name in new[] { "first", "second", "third" })
        {
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be(name, typeof(TEnum).Name);
        }

        reader.Read().Should().BeFalse();
    }

    private void IsRefused<TEnum>(bool boxed, params TEnum[] values) where TEnum : struct, Enum
    {
        var table = CreateTable<TEnum>();

        foreach (var value in values)
        {
            Connection.Invoking(c =>
            {
                using var appender = c.CreateAppender(table);
                Append(appender.CreateRow(), value, boxed).EndRow();
            }).Should().Throw<InvalidOperationException>($"{typeof(TEnum).Name}.{value}").WithMessage("*outside the range (0-2)*");
        }
    }

    private static IDuckDBAppenderRow Append<TEnum>(IDuckDBAppenderRow row, TEnum value, bool boxed) where TEnum : struct, Enum
    {
        return boxed ? row.AppendValue((Enum)value) : row.AppendValue(value);
    }

    private string CreateTable<TEnum>()
    {
        var name = typeof(TEnum).Name.ToLowerInvariant();

        Connection.ExecuteNonQuery($"DROP TABLE IF EXISTS enumWrite_{name}; DROP TYPE IF EXISTS enumWrite_{name}_type; " +
                                   $"CREATE TYPE enumWrite_{name}_type AS ENUM ('first', 'second', 'third'); " +
                                   $"CREATE TABLE enumWrite_{name} (value enumWrite_{name}_type)");

        return $"enumWrite_{name}";
    }
}
