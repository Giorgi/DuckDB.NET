using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class HugeIntTypeHandler : BaseTypeHandler
    {
        public override Type ClrType { get; } = typeof(BigInteger);

        public unsafe HugeIntTypeHandler(IntPtr vector, void* dataPointer, ulong* validityMaskPointer)
            : base(vector, dataPointer, validityMaskPointer) { }

        protected unsafe internal DuckDBHugeInt GetNative(ulong offset)
            => GetFieldData<DuckDBHugeInt>(offset);

        public BigInteger GetBigInteger(ulong offset)
            => GetNative(offset).ToBigInteger();
        
        public override T GetValue<T>(ulong offset)
        {
            var bigInteger = GetBigInteger(offset);
            if (typeof(T) == typeof(BigInteger))
                return Unsafe.As<BigInteger, T>(ref bigInteger);
            else
                return Convert<T>(bigInteger);
        }
    }
}
