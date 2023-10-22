using System;
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

        public T? GetValue<T>(ulong offset)
        {
            var value = GetValue(offset);
            return value is T typed ? typed : (T?) TypeDescriptor.GetConverter(typeof(T)).ConvertFrom(value);
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
