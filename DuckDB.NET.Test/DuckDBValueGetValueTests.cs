namespace DuckDB.NET.Test;

// DuckDBValue.GetValue<T> reads a parameter of a table function. It gives the value as the type it makes of the
// DuckDB type, or as object, a nullable or an interface of it, and refuses any other type. Before 2.0 it reinterpreted
// the value as T: a long read 8 bytes of a 4-byte INTEGER, and a string took the integer for a reference.
public class DuckDBValueGetValueTests
{
    [Fact]
    public void AValueReadsAsItsOwnType()
    {
        using var integer = NativeMethods.Value.DuckDBCreateInt32(42);
        integer.GetValue<int>().Should().Be(42);

        using var text = NativeMethods.Value.DuckDBCreateVarchar("duck");
        text.GetValue<string>().Should().Be("duck");

        using var huge = NativeMethods.Value.DuckDBCreateHugeInt(new DuckDBHugeInt((Int128)42));
        huge.GetValue<Int128>().Should().Be(42);
        huge.GetValue<BigInteger>().Should().Be(new BigInteger(42));
    }

    [Fact]
    public void AValueReadsAsObjectNullableAndInterface()
    {
        using var integer = NativeMethods.Value.DuckDBCreateInt32(42);
        integer.GetValue<object>().Should().Be(42);
        integer.GetValue<int?>().Should().Be(42);
        integer.GetValue<IComparable<int>>().CompareTo(42).Should().Be(0);

        using var text = NativeMethods.Value.DuckDBCreateVarchar("duck");
        text.GetValue<object>().Should().Be("duck");
    }

    [Fact]
    public void AValueIsRefusedAsAnotherType()
    {
        using var integer = NativeMethods.Value.DuckDBCreateInt32(42);
        integer.Invoking(v => v.GetValue<long>()).Should().Throw<InvalidCastException>().WithMessage("Cannot read a value of type Int32 as Int64.");
        integer.Invoking(v => v.GetValue<short>()).Should().Throw<InvalidCastException>();
        integer.Invoking(v => v.GetValue<string>()).Should().Throw<InvalidCastException>().WithMessage("Cannot read a value of type Int32 as String.");

        using var text = NativeMethods.Value.DuckDBCreateVarchar("duck");
        text.Invoking(v => v.GetValue<int>()).Should().Throw<InvalidCastException>();
    }
}
