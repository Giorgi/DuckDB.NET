using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Globalization;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class ListTypeHandler : BaseTypeHandler
    {
        private ITypeHandler InternalTypeHandler { get; }
        private DuckDBLogicalType LogicalType { get; }

        public override Type ClrType => typeof(List<>).MakeGenericType(InternalTypeHandler.ClrType);

        public unsafe ListTypeHandler(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, ITypeHandlerFactory factory)
            : base(vector, dataPointer, validityMaskPointer)
        {
            LogicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(vector);
            using var childType = NativeMethods.LogicalType.DuckDBListTypeChildType(LogicalType);

            var type = NativeMethods.LogicalType.DuckDBGetTypeId(childType);
            var childVector = NativeMethods.DataChunks.DuckDBListVectorGetChild(vector);
            var childVectorData = NativeMethods.DataChunks.DuckDBVectorGetData(childVector);
            var childVectorValidity = NativeMethods.DataChunks.DuckDBVectorGetValidity(childVector);

            InternalTypeHandler = factory.Instantiate(childVector, childVectorData, childVectorValidity, type);
        }

        public override object GetValue(ulong offset)
        {
            var listType = typeof(List<>).MakeGenericType(InternalTypeHandler.ClrType);
            return GetList(offset, listType);
        }

        public override object GetValue(ulong offset, Type type)
        {
            var listType = typeof(List<>).MakeGenericType(type.GetGenericArguments()[0]);
            var value = GetList(offset, listType);
            var converted = Convert(value, type);
            return converted;
        }

        public override T GetValue<T>(ulong offset)
            => (T)GetList(offset, typeof(T));

        public T GetList<T>(ulong offset) where T : IList
            => (T)GetList(offset, typeof(T));

        internal unsafe object GetList(ulong offset, Type returnType)
        {
            if (!typeof(IList).IsAssignableFrom(returnType))
            {
                throw new InvalidOperationException($"Cannot return a list with a {nameof(returnType)} not implementing IList.");
            }

            var genericArgs = returnType.GetGenericArguments();
            if (genericArgs.Length == 0)
                throw new InvalidOperationException($"Cannot return a list with a {nameof(returnType)} not implementing IList<>.");
            var listType = genericArgs[0];

            var listData = (DuckDBListEntry*)DataPointer + offset;
            
            var nullableType = Nullable.GetUnderlyingType(listType);
            var allowNulls = !listType.IsValueType || nullableType != null;

            var list = (IList)Activator.CreateInstance(returnType)!;

            var targetType = nullableType ?? listType;

            for (ulong i = 0; i < listData->Length; i++)
            {
                var childOffset = i + listData->Offset;
                if (InternalTypeHandler.IsValid(childOffset))
                {
                    var item = InternalTypeHandler.GetValue(childOffset, targetType);
                    list.Add(item);
                }
                else
                {
                    if (allowNulls)
                    {
                        list.Add(null);
                    }
                    else
                    {
                        throw new SqlNullValueException("The list contains null value");
                    }
                }
            }

            return list;
        }

        public override void Dispose()
        {
            InternalTypeHandler?.Dispose();
            LogicalType?.Dispose();
            base.Dispose();
        }
        
    }
}