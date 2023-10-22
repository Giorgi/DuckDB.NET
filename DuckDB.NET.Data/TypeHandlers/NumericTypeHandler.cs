using System;
using System.Collections.Generic;
using System.Linq;
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

        protected unsafe internal T GetNative(ulong offset)
            => GetFieldData<T>(offset);

        public override object GetValue(ulong offset)
            => GetNative(offset);

        public decimal GetDecimal(ulong offset)
            => Convert<decimal>(GetValue(offset));
    }
}
