using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class TimeTypeHandler : BaseTypeHandler, IReadDateTimeTypeHandler
    {
        public override Type ClrType { get => typeof(DuckDBTimeOnly); }

        public unsafe TimeTypeHandler(IntPtr vector, void* dataPointer, ulong* validityMaskPointer)
            : base(vector, dataPointer, validityMaskPointer) { }

        protected internal DuckDBTimeOnly GetNative(ulong offset)
            => NativeMethods.DateTime.DuckDBFromTime(GetFieldData<DuckDBTime>(offset));

        public override object GetValue(ulong offset)
            => GetNative(offset);

        protected override object Convert(object value, Type type)
        {
            return value switch
            {
                DuckDBTimeOnly x when typeof(DateTime).IsAssignableFrom(type) => x.ToDateTime(),
#if NET6_0_OR_GREATER
                DuckDBTimeOnly x when typeof(TimeOnly).IsAssignableFrom(type) => (TimeOnly)x,
#endif
                _ => base.Convert(value, type),
            };
        }

#if NET6_0_OR_GREATER
        public TimeOnly GetTimeOnly(ulong offset)
            => (TimeOnly)GetNative(offset);
#endif

        public DateTime GetDateTime(ulong offset)
            => (DateTime)GetNative(offset);

        public override T GetValue<T>(ulong offset)
            => throw new NotImplementedException();
    }
}
