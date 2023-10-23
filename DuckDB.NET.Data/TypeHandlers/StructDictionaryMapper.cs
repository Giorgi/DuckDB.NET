using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Globalization;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class StructDictionaryMapper : IStructMapper<Dictionary<string, object?>>
    {
        private IDictionary<string, ITypeHandler> InternalTypeHandlers { get; }
        public StructDictionaryMapper(IDictionary<string, ITypeHandler> internalTypeHandlers)
            => InternalTypeHandlers = internalTypeHandlers;

        public Dictionary<string, object?> Map(ulong offset)
        {
            var result = new Dictionary<string, object?>();
            foreach (var reader in InternalTypeHandlers)
            {
                var value = reader.Value.IsValid(offset) ? reader.Value.GetValue(offset) : null;
                result.Add(reader.Key, value);
            }
            return result;
        }

        object IStructMapper.Map(ulong offset) => Map(offset)!;
    }
}