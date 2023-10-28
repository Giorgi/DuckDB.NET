using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class DateTypeHandler : BaseTypeHandler, IReadDateTimeTypeHandler
    {
        public override Type ClrType { get => typeof(DuckDBDateOnly); }

        public unsafe DateTypeHandler(IntPtr vector, void* dataPointer, ulong* validityMaskPointer)
            : base(vector, dataPointer, validityMaskPointer) { }

        protected internal DuckDBDateOnly GetNative(ulong offset)
            => NativeMethods.DateTime.DuckDBFromDate(GetFieldData<DuckDBDate>(offset));

        protected override object Convert(object value, Type type)
        {
            return value switch
            {
                DuckDBDateOnly x when typeof(DateTime).IsAssignableFrom(type) => x.ToDateTime(),
#if NET6_0_OR_GREATER
                DuckDBDateOnly x when typeof(DateOnly).IsAssignableFrom(type) => (DateOnly)x,
#endif
                _ => base.Convert(value, type),
            };
        }

#if NET6_0_OR_GREATER
        public DateOnly GetDateOnly(ulong offset)
            => (DateOnly)GetNative(offset);
#endif
        
        public DateTime GetDateTime(ulong offset)
            => (DateTime)GetNative(offset);

        public override T GetValue<T>(ulong offset)
            => throw new NotImplementedException();
    }
}
