using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class EnumTypeHandler : BaseTypeHandler
    {
        private ITypeHandler InternalTypeHandler { get; }
        private DuckDBLogicalType LogicalType { get; }

        public override Type ClrType { get => typeof(string); }

        public unsafe EnumTypeHandler(IntPtr vector, void* dataPointer, ulong* validityMaskPointer)
            : base(vector, dataPointer, validityMaskPointer)
        {
            LogicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(vector);
            var enumType = NativeMethods.LogicalType.DuckDBEnumInternalType(LogicalType);
            InternalTypeHandler = enumType switch
            {
                DuckDBType.UnsignedTinyInt => new NumericTypeHandler<byte>(vector, dataPointer, validityMaskPointer),
                DuckDBType.UnsignedSmallInt => new NumericTypeHandler<short>(vector, dataPointer, validityMaskPointer),
                DuckDBType.UnsignedInteger => new NumericTypeHandler<int>(vector, dataPointer, validityMaskPointer),
                _ => throw new NotSupportedException()
            };
        }

        public string GetString(ulong offset)
        {
            var value = NativeMethods.LogicalType.DuckDBEnumDictionaryValue(LogicalType, GetLong(offset)).ToManagedString();
            return value;
        }

        protected long GetLong(ulong offset)
        {
            var value = InternalTypeHandler.GetValue(offset);
            return Convert<long>(value);
        }

        public T? GetEnum<T>(ulong offset) where T : Enum
            => (T?)GetEnum(offset, typeof(T));

        public object? GetEnum(ulong offset, Type type)
        {
            var underlyingType = Nullable.GetUnderlyingType(type);
            if (underlyingType != null)
            {
                if (!IsValid(offset))
                {
                    return default!;
                }
                type = underlyingType;
            }

            var enumItem = Enum.Parse(type, GetLong(offset).ToString(CultureInfo.InvariantCulture));
            return enumItem;
        }

        public override void Dispose()
        {
            InternalTypeHandler?.Dispose();
            LogicalType?.Dispose();
            base.Dispose();
        }
        public override T GetValue<T>(ulong offset)
            => throw new NotImplementedException();
    }
}