using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using static System.Collections.Specialized.BitVector32;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class ListTypeHandler : BaseTypeHandler
    {
        private ITypeHandler InternalTypeHandler { get; }
        private DuckDBLogicalType LogicalType { get; }

        private KeyValuePair<Type, Delegate> ListCache { get; set; } = new();
        private KeyValuePair<Type, Delegate> ItemCache { get; set; } = new();

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
            return GetValue(offset, listType);
        }

        public override object GetValue(ulong offset, Type type)
        {
            var listType = typeof(List<>).MakeGenericType(type.GetGenericArguments()[0]);
            if (ListCache.Key != listType)
            {
                var methodInfo = typeof(ListTypeHandler)
                                            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
                                            .Where(x => x.Name==nameof(GetValue))
                                            .Where(x => x.GetParameters().Length==1)
                                            .Where(x => x.ContainsGenericParameters)
                                            .First()
                                            .MakeGenericMethod(new[] { listType });

                var param = Expression.Parameter(typeof(ulong));
                var callRef = Expression.Call(Expression.Constant(this), methodInfo, param);
                var lambda = Expression.Lambda(callRef, new[] { param });
                var compiled = lambda.Compile();
                ListCache = new(listType, compiled);
            }
            var expression = (Func<ulong, object>)ListCache.Value;
            var value = expression.Invoke(offset);
            //var converted = Convert(value, type);
            //return converted;
            return value;
        }

        public override T GetValue<T>(ulong offset)
        {
            if (ItemCache.Key != typeof(T))
            { 
                if (!typeof(IList).IsAssignableFrom(typeof(T)))
                    throw new InvalidOperationException($"Cannot return a list with a {nameof(T)} not implementing IList.");

                var genericArgs = typeof(T).GetGenericArguments();
                if (genericArgs.Length == 0)
                    throw new InvalidOperationException($"Cannot return a list with a {nameof(T)} not implementing IList<>.");
                var listType = genericArgs[0];

                var nullableType = Nullable.GetUnderlyingType(listType);
                var allowNulls = !listType.IsValueType || nullableType != null;
                var targetType = nullableType ?? listType;

                var methodInfo = typeof(ListTypeHandler)
                                    .GetMethod(nameof(GetList), BindingFlags.Instance | BindingFlags.NonPublic)!
                                    .MakeGenericMethod(new[] { typeof(T), listType });

                var param = Expression.Parameter(typeof(ulong));
                var callRef = Expression.Call(Expression.Constant(this), methodInfo, param, Expression.Constant(allowNulls));
                var lambda = Expression.Lambda(callRef, new[] { param });
                var compiled = lambda.Compile();
                ItemCache = new (targetType, compiled);
            }
            var expression = (Func<ulong, T>)ItemCache.Value;
            var value = expression.Invoke(offset);
            return value;
        }

        protected unsafe T GetList<T, U>(ulong offset, bool allowNulls) where T : IList<U?>
        {
            var listData = (DuckDBListEntry*)DataPointer + offset;
            var list = Activator.CreateInstance<T>()!;
            
            for (ulong i = 0; i < listData->Length; i++)
            {
                var childOffset = i + listData->Offset;
                if (InternalTypeHandler.IsValid(childOffset))
                {
                    var item = InternalTypeHandler.GetValue<U>(childOffset);
                    list.Add(item);
                }
                else
                {
                    if (allowNulls)
                    {
                        list.Add(default);
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