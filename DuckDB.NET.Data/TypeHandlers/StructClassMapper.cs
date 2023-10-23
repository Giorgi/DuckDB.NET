using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class StructClassMapper<T> : IStructMapper<T>
    {
        private IDictionary<string, ITypeHandler> InternalTypeHandlers { get; }
        public StructClassMapper(IDictionary<string, ITypeHandler> internalTypeHandlers)
            => InternalTypeHandlers = internalTypeHandlers;

        public T Map(ulong offset)
        {
            var result = (T)Activator.CreateInstance(typeof(T))!;
            foreach (var propertyInfo in typeof(T).GetProperties().Where(x => x.SetMethod != null))
            {
                var isNotNullable = propertyInfo.PropertyType.IsValueType
                            && Nullable.GetUnderlyingType(propertyInfo.PropertyType) == null;
                if (InternalTypeHandlers.TryGetValue(propertyInfo.Name, out var typeHandler))
                {
                    if (typeHandler.IsValid(offset))
                    {
                        var value = typeHandler.GetValue(offset, propertyInfo.PropertyType);
                        propertyInfo.SetValue(result, value);
                    }
                    else if (isNotNullable)
                        throw new NullReferenceException($"Property '{propertyInfo.Name}' is not nullable but struct contains null");
                    
                }
                else if(isNotNullable)
                    throw new NullReferenceException($"Property '{propertyInfo.Name}' not found in struct");
            }
            return result;
        }

        object IStructMapper.Map(ulong offset) => Map(offset)!;
    }
}