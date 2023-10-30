using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class IntervalTypeHandler : BaseTypeHandler
    {
        public override Type ClrType { get; } = typeof(DuckDBInterval);

        public unsafe IntervalTypeHandler(IntPtr vector, void* dataPointer, ulong* validityMaskPointer)
            : base(vector, dataPointer, validityMaskPointer) { }

        protected internal DuckDBInterval GetNative(ulong offset)
            => GetFieldData<DuckDBInterval>(offset);

        public TimeSpan GetTimeSpan(ulong offset)
            => (TimeSpan)GetNative(offset);
        
        public override T GetValue<T>(ulong offset)
        {
            var duckDbInterval = GetNative(offset);
            if (typeof(T) == typeof(DuckDBInterval))
                return Unsafe.As<DuckDBInterval, T>(ref duckDbInterval);
            else
                return Convert<T>(duckDbInterval);
        }
    }
}
