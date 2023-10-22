using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class IntervalTypeHandler : BaseTypeHandler
    {
        public override Type ClrType { get => typeof(DuckDBInterval); }

        public unsafe IntervalTypeHandler(IntPtr vector, void* dataPointer, ulong* validityMaskPointer)
            : base(vector, dataPointer, validityMaskPointer) { }

        protected internal DuckDBInterval GetNative(ulong offset)
            => GetFieldData<DuckDBInterval>(offset);

        public override object GetValue(ulong offset)
            => GetNative(offset);

        public TimeSpan GetTimeSpan(ulong offset)
            => (TimeSpan)GetNative(offset);
    }
}
