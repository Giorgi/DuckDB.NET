using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class NumericTypeHandler<T> : BaseTypeHandler, IReadDecimalTypeHandler where T : unmanaged
    {
        public override Type ClrType { get => typeof(T); }

        private Type[] NumericTypes = new[] {
            typeof(bool)
            , typeof(sbyte), typeof(byte)
            , typeof(short), typeof(ushort)
            , typeof(int), typeof(uint)
            , typeof(long), typeof(ulong)
            , typeof(float), typeof(double)
        };

        public unsafe NumericTypeHandler(IntPtr vector, void* dataPointer, ulong* validityMaskPointer)
            : base(vector, dataPointer, validityMaskPointer) 
        { 
           if (!NumericTypes.Contains(typeof(T)))
                throw new NotSupportedException();
        }

        public override U GetValue<U>(ulong offset)
        {
            var numeric = GetFieldData<T>(offset);
            if (typeof(T) == typeof(U))
                return Unsafe.As<T, U>(ref numeric);
            else
                return Convert<U>(numeric);
        }

        public decimal GetDecimal(ulong offset)
            => Convert<decimal>(GetValue<T>(offset));
    }
}
