using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class TimestampTypeHandler : BaseTypeHandler, IReadDateTimeTypeHandler
    {
        public override Type ClrType { get; } = typeof(DateTime);

        public unsafe TimestampTypeHandler(IntPtr vector, void* dataPointer, ulong* validityMaskPointer)
            : base(vector, dataPointer, validityMaskPointer) { }

        protected unsafe internal DuckDBTimestamp GetNative(ulong offset)
            => NativeMethods.DateTime.DuckDBFromTimestamp(GetFieldData<DuckDBTimestampStruct>(offset));

        public override object GetValue(ulong offset)
            => GetDateTime(offset);

        public DateTime GetDateTime(ulong offset)
            => GetNative(offset).ToDateTime();
        
        public override T GetValue<T>(ulong offset)
        {
            var dateTime = GetDateTime(offset);
            if (typeof(T) == typeof(DateTime))
                return Unsafe.As<DateTime, T>(ref dateTime);
            else
                return Convert<T>(dateTime);
        }
    }
}
