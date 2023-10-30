using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
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
        private KeyValuePair<Type, Delegate> Cache { get; set; } = new();

        public unsafe BaseTypeHandler(IntPtr vector, void* dataPointer, ulong* validityMaskPointer)
        {
            Vector = vector;
            DataPointer = dataPointer;
            ValidityMaskPointer = validityMaskPointer;
        }

        public abstract T GetValue<T>(ulong offset);
        
        public virtual object GetValue(ulong offset, Type type)
        {
            if (Cache.Key != type)
            {
                var methodInfo = GetType()
                                    .GetMethods(BindingFlags.Instance | BindingFlags.Public)
                                    .Where(x => x.Name == nameof(GetValue))
                                    .Where(x => x.GetParameters().Length == 1)
                                    .Where(x => x.ContainsGenericParameters)
                                    .First()
                                    .MakeGenericMethod(new[] { type });

                var param = Expression.Parameter(typeof(ulong));
                var callRef = Expression.Call(Expression.Constant(this), methodInfo, param);
                var lambda = Expression.Lambda(callRef, new[] { param });
                var compiled = lambda.Compile();
                Cache = new(type, compiled);
            }
            var expression = Cache.Value;
            var value = expression.DynamicInvoke(offset);
            return value!;
        }

        public virtual object GetValue(ulong offset)
            => GetValue(offset, ClrType);

        protected virtual T Convert<T>(object value)
        {
            if (typeof(T).IsAssignableFrom(value.GetType()))
                return (T)value;
            return (T)Convert(value, typeof(T));
        }

        protected virtual object Convert(object value, Type type)
        {
            if (type.IsAssignableFrom(value.GetType()))
                return value;
            if (TypeDescriptor.GetConverter(type).CanConvertFrom(value.GetType()))
                return TypeDescriptor.GetConverter(type).ConvertFrom(value)!;
            return System.Convert.ChangeType(value, type);
        }

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
