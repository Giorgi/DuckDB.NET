using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Globalization;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class StructTypeHandler : BaseTypeHandler
    {
        private IDictionary<string, ITypeHandler> InternalTypeHandlers { get; }
        private DuckDBLogicalType LogicalType { get; }

        public override Type ClrType => typeof(Dictionary<string, object>);

        public unsafe StructTypeHandler(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, ITypeHandlerFactory factory)
            : base(vector, dataPointer, validityMaskPointer)
        {
            InternalTypeHandlers = new Dictionary<string, ITypeHandler>(StringComparer.OrdinalIgnoreCase);

            LogicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(vector);
            var memberCount = NativeMethods.LogicalType.DuckDBStructTypeChildCount(LogicalType);
            for (int index = 0; index < memberCount; index++)
            {
                var name = NativeMethods.LogicalType.DuckDBStructTypeChildName(LogicalType, index).ToManagedString();
                var childVector = NativeMethods.DataChunks.DuckDBStructVectorGetChild(vector, index);
                var childVectorData = NativeMethods.DataChunks.DuckDBVectorGetData(childVector);
                var childVectorValidity = NativeMethods.DataChunks.DuckDBVectorGetValidity(childVector);

                using var childType = NativeMethods.LogicalType.DuckDBStructTypeChildType(LogicalType, index);
                var type = NativeMethods.LogicalType.DuckDBGetTypeId(childType);

                InternalTypeHandlers.Add(name, factory.Instantiate(childVector, childVectorData, childVectorValidity, type));
            }
        }

        public override object GetValue(ulong offset)
        {
            var dictType = typeof(Dictionary<string, object?>);
            return GetStruct(offset, dictType);
        }

        public override object GetValue(ulong offset, Type type)
            => GetStruct(offset, type);

        public override T GetValue<T>(ulong offset)
            => (T)GetStruct(offset, typeof(T));

        public T GetStruct<T>(ulong offset)
            => (T)GetStruct(offset, typeof(T));

        public object GetStruct(ulong offset, Type returnType)
        {
            var mapperType = typeof(IDictionary<string, object?>).IsAssignableFrom(returnType)
                                ? typeof(StructDictionaryMapper)
                                : typeof(StructClassMapper<>).MakeGenericType(returnType);
            var mapper = (IStructMapper)Activator.CreateInstance(mapperType, new[] { InternalTypeHandlers })!;
            return mapper.Map(offset);
        }

        public override void Dispose()
        {
            LogicalType?.Dispose();
            base.Dispose();
        }
    }
}