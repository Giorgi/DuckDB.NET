using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal abstract class BaseTypeHandler : ITypeHandler
    {
        protected const int INLINE_STRING_MAX_LENGTH = 12;
        protected IntPtr Vector { get; }
        protected unsafe void* DataPointer { get; }
        private unsafe ulong* ValidityMaskPointer { get; }
        public abstract Type ClrType { get; }

        public unsafe BaseTypeHandler(IntPtr vector, void* dataPointer, ulong* validityMaskPointer)
        {
            Vector = vector;
            DataPointer = dataPointer;
            ValidityMaskPointer = validityMaskPointer;
        }

        public virtual T GetValue<T>(ulong offset)
        {
            var value = GetValue(offset);
            return Convert<T>(value);
        }

        public virtual object GetValue(ulong offset, Type type)
        {
            var value = GetValue(offset);
            var converted = Convert(value, type);
            return converted;
        }

        protected virtual T Convert<T>(object value)
            => (T)Convert(value, typeof(T));

        protected virtual object Convert(object value, Type type)
        {
            if (type.IsAssignableFrom(value.GetType()))
                return value;
            if (TypeDescriptor.GetConverter(type).CanConvertFrom(value.GetType()))
                return TypeDescriptor.GetConverter(type).ConvertFrom(value)!;
            return System.Convert.ChangeType(value, type);
        }

        public abstract object GetValue(ulong offset);

        protected unsafe T GetFieldData<T>(ulong offset) where T : unmanaged
            => *((T*)DataPointer + offset);

        public unsafe bool IsValid(ulong offset)
        {
            var validityMaskEntryIndex = offset / 64;
            var validityBitIndex = (int)(offset % 64);

            var validityMaskEntryPtr = ValidityMaskPointer + validityMaskEntryIndex;
            var validityBit = 1ul << validityBitIndex;

            var isValid = (*validityMaskEntryPtr & validityBit) != 0;
            return isValid;
        }

        public virtual void Dispose()
        { }
    }
}
