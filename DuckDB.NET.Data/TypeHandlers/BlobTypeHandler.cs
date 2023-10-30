using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class BlobTypeHandler : BaseTypeHandler, IStreamTypeHandler
    {
        public override Type ClrType { get; } = typeof(Stream);

        public unsafe BlobTypeHandler(IntPtr vector, void* dataPointer, ulong* validityMaskPointer)
            : base(vector, dataPointer, validityMaskPointer) { }

        protected internal DuckDBBlob GetNative(ulong offset)
            => GetFieldData<DuckDBBlob>(offset);

        public override object GetValue(ulong offset)
            => GetNative(offset);

        public unsafe Stream GetStream(ulong offset)
        {
            var data = (DuckDBString*)DataPointer + offset;
            var length = *(int*)data;

            if (length <= INLINE_STRING_MAX_LENGTH)
            {
                var value = new string(data->value.inlined.inlined, 0, length, Encoding.UTF8);
                return new MemoryStream(Encoding.UTF8.GetBytes(value), false);
            }

            return new UnmanagedMemoryStream((byte*)data->value.pointer.ptr, length, length, FileAccess.Read);
        }

        public override T GetValue<T>(ulong offset)
            => throw new NotImplementedException();
    }
}
