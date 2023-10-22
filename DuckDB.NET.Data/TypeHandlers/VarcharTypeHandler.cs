﻿using System;
using System.Collections.Generic;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class VarcharTypeHandler : BaseTypeHandler, IReadStringTypeHandler
    {
        public override Type ClrType { get => typeof(string); }

        public unsafe VarcharTypeHandler(IntPtr vector, void* dataPointer, ulong* validityMaskPointer)
            : base(vector, dataPointer, validityMaskPointer) { }

        public unsafe string GetString(ulong offset)
        {
            var data = (DuckDBString*)DataPointer + offset;
            var length = *(int*)data;

            var pointer = length <= INLINE_STRING_MAX_LENGTH
                ? data->value.inlined.inlined
                : data->value.pointer.ptr;

            return new string(pointer, 0, length, Encoding.UTF8);
        }

        public override object GetValue(ulong offset)
            => GetString(offset);
    }
}
