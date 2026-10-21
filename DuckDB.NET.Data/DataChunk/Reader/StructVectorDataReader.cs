using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using DuckDB.NET.Data.Common;

namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class StructVectorDataReader : VectorDataReaderBase
{
    // The settable properties of each target type, worked out once per process. TypedCache holds a typed reader per
    // property, with no field reader yet, where code can be made at run time; TypeCache holds the boxed setters used
    // where it cannot.
    private static readonly ConcurrentDictionary<Type, PropertyReader[]?> TypedCache = new();
    private static readonly ConcurrentDictionary<Type, TypeDetails> TypeCache = new();
    // TODO: Replace with OrderedDictionary<string, VectorDataReaderBase> when .NET 8 support is dropped.
    // The parallel array exists only to guarantee field-ordinal iteration in Reset/Dispose.
    private readonly Dictionary<string, VectorDataReaderBase> structDataReaders;
    private readonly VectorDataReaderBase[] orderedReaders;

    // The properties of the type GetStruct was asked for last, each with the reader of its field. A column is read as
    // the same type row after row, so each property is matched with its field once and not for every row.
    private Type? pairedType;
    private PropertyReader[] propertyReaders = [];

    internal unsafe StructVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        using var logicalType = NativeMethods.Vectors.DuckDBVectorGetColumnType(vector);
        var memberCount = NativeMethods.LogicalType.DuckDBStructTypeChildCount(logicalType);
        structDataReaders = new Dictionary<string, VectorDataReaderBase>((int)memberCount, StringComparer.OrdinalIgnoreCase);
        orderedReaders = new VectorDataReaderBase[memberCount];

        for (int index = 0; index < memberCount; index++)
        {
            var name = NativeMethods.LogicalType.DuckDBStructTypeChildName(logicalType, index);
            var childVector = NativeMethods.Vectors.DuckDBStructVectorGetChild(vector, index);

            using var childType = NativeMethods.LogicalType.DuckDBStructTypeChildType(logicalType, index);
            var reader = VectorDataReaderFactory.CreateReader(childVector, childType, columnName);
            structDataReaders[name] = reader;
            orderedReaders[index] = reader;
        }
    }

    internal override object GetValueCore(ulong offset, Type targetType)
    {
        if (DuckDBType == DuckDBType.Struct)
        {
            return GetStruct(offset, targetType);
        }

        return base.GetValueCore(offset, targetType);
    }

    private object GetStruct(ulong offset, Type returnType)
    {
        var result = Activator.CreateInstance(returnType);

        if (result is Dictionary<string, object?> dictionary)
        {
            foreach (var reader in structDataReaders)
            {
                var value = reader.Value.IsValid(offset) ? reader.Value.GetValue(offset) : null;
                dictionary.Add(reader.Key, value);
            }

            return result;
        }

        if (returnType != pairedType)
        {
            propertyReaders = PairPropertiesWithReaders(returnType);
            pairedType = returnType;
        }

        // A C# struct is set inside its box, which is what is returned.
        foreach (var propertyReader in propertyReaders)
        {
            propertyReader.Read(result!, offset);
        }

        return result!;
    }

    // In the order of the properties of the type. A property with no field of its name is left as it is.
    private PropertyReader[] PairPropertiesWithReaders(Type returnType)
    {
        var pairs = new List<PropertyReader>();

        // A static lambda: one that used returnType allocated a closure and a delegate for every call.
        var prototypes = TypedCache.GetOrAdd(returnType, static type => CreateTypedReaders(type));

        if (prototypes != null)
        {
            foreach (var prototype in prototypes)
            {
                if (structDataReaders.TryGetValue(prototype.Name, out var reader))
                {
                    pairs.Add(prototype.WithReader(reader));
                }
            }

            return [.. pairs];
        }

        var typeDetails = TypeCache.GetOrAdd(returnType, static type => CreateTypeDetails(type));

        foreach (var property in typeDetails.Properties)
        {
            if (structDataReaders.TryGetValue(property.Key, out var reader))
            {
                pairs.Add(new BoxedPropertyReader(property.Key, property.Value, reader));
            }
        }

        return [.. pairs];
    }

    // The public properties that have a set method, whatever its access.
    private static List<PropertyInfo> SettableProperties(Type type)
    {
        var properties = new List<PropertyInfo>();

        foreach (var property in type.GetProperties())
        {
            if (property.SetMethod != null)
            {
                properties.Add(property);
            }
        }

        return properties;
    }

    // A reader per property, generic over the type that declares the property and the type of the property, so that a
    // field is read with ReadValidValue (the conversion of GetFieldValue<T> on a column) and given to the setter
    // without a box. The setter is a delegate made from the set method itself; nothing is compiled.
    // Null where no code can be made at run time: the types are the caller's, so none can be named in advance.
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(ClassPropertyReader<,>))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(NullableClassPropertyReader<,>))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(StructPropertyReader<,>))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(NullableStructPropertyReader<,>))]
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "As IL2076, reported by the .NET 8 analyzer.")]
    [UnconditionalSuppressMessage("Trimming", "IL2076", Justification = "The new() constraint that 'where T : struct' implies is met by every value type, and the readers' constructors are kept by the DynamicDependency attributes. The MakeGenericType calls run only where dynamic code is supported.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "The MakeGenericType calls run only where RuntimeFeature.IsDynamicCodeSupported is true, tested in this method; the .NET 8 analyzer does not recognise that guard.")]
    private static PropertyReader[]? CreateTypedReaders(Type type)
    {
        if (!RuntimeFeature.IsDynamicCodeSupported)
        {
            return null;
        }

        // By name, as the boxed setters are: two properties of the same name fail in the same way.
        var readers = new Dictionary<string, PropertyReader>();

        foreach (var property in SettableProperties(type))
        {
            var setMethod = property.SetMethod!;
            var declaringType = setMethod.DeclaringType!;
            var underlyingType = Nullable.GetUnderlyingType(property.PropertyType);

            var readerType = (declaringType.IsValueType, underlyingType != null) switch
            {
                (false, false) => typeof(ClassPropertyReader<,>).MakeGenericType(declaringType, property.PropertyType),
                (false, true) => typeof(NullableClassPropertyReader<,>).MakeGenericType(declaringType, underlyingType!),
                (true, false) => typeof(StructPropertyReader<,>).MakeGenericType(declaringType, property.PropertyType),
                (true, true) => typeof(NullableStructPropertyReader<,>).MakeGenericType(declaringType, underlyingType!)
            };

            // A set method that is static or takes an index has no delegate of this form: that throws, as the
            // boxed setter does.
            var reader = Activator.CreateInstance(readerType, BindingFlags.Public | BindingFlags.Instance | BindingFlags.DoNotWrapExceptions, null, [property.Name, setMethod], null);
            readers.Add(property.Name, (PropertyReader)reader!);
        }

        return [.. readers.Values];
    }

    private static TypeDetails CreateTypeDetails(Type type)
    {
        var details = new TypeDetails();

        foreach (var propertyInfo in SettableProperties(type))
        {
            var isNullable = propertyInfo.PropertyType.AllowsNullValue(out var isNullableValueType, out var underlyingType);

            // For a C# struct, the compiled setter would unbox it and set a copy; this path runs only without dynamic
            // code, where Compile interprets the expression and sets the struct inside the box.
            var instanceParam = Expression.Parameter(typeof(object));
            var argumentParam = Expression.Parameter(typeof(object));

            var setAction = Expression.Lambda<Action<object, object>>(
                Expression.Call(Expression.Convert(instanceParam, type), propertyInfo.SetMethod!, Expression.Convert(argumentParam, propertyInfo.PropertyType)),
                instanceParam, argumentParam
            ).Compile();

            details.Properties.Add(propertyInfo.Name, new PropertyDetails(propertyInfo.PropertyType, isNullable, isNullableValueType, underlyingType, setAction));
        }

        return details;
    }

    // Reads one field of a row into one property of the target. The readers in TypedCache have no field reader;
    // WithReader gives a copy that reads the field of this column.
    private abstract class PropertyReader(string name)
    {
        public string Name { get; } = name;

        public abstract PropertyReader WithReader(VectorDataReaderBase reader);

        public abstract void Read(object target, ulong offset);

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        protected void ThrowNotNullable() => throw new InvalidCastException($"Property '{Name}' is not nullable but struct contains null value");
    }

    // A NULL field leaves a property that can hold null as it is, and is refused for one that cannot.
    private sealed class ClassPropertyReader<TTarget, TProperty>(string name, Action<TTarget, TProperty> setter, VectorDataReaderBase? reader)
        : PropertyReader(name) where TTarget : class
    {
        public ClassPropertyReader(string name, MethodInfo setMethod) : this(name, setMethod.CreateDelegate<Action<TTarget, TProperty>>(), null)
        {
        }

        public override PropertyReader WithReader(VectorDataReaderBase fieldReader) => new ClassPropertyReader<TTarget, TProperty>(Name, setter, fieldReader);

        public override void Read(object target, ulong offset)
        {
            if (reader!.IsValid(offset))
            {
                setter(Unsafe.As<TTarget>(target), reader.ReadValidValue<TProperty>(offset));
            }
            else if (typeof(TProperty).IsValueType)
            {
                ThrowNotNullable();
            }
        }
    }

    private sealed class NullableClassPropertyReader<TTarget, TValue>(string name, Action<TTarget, TValue?> setter, VectorDataReaderBase? reader)
        : PropertyReader(name) where TTarget : class where TValue : struct
    {
        public NullableClassPropertyReader(string name, MethodInfo setMethod) : this(name, setMethod.CreateDelegate<Action<TTarget, TValue?>>(), null)
        {
        }

        public override PropertyReader WithReader(VectorDataReaderBase fieldReader) => new NullableClassPropertyReader<TTarget, TValue>(Name, setter, fieldReader);

        public override void Read(object target, ulong offset)
        {
            if (reader!.IsValid(offset))
            {
                setter(Unsafe.As<TTarget>(target), reader.ReadValidValue<TValue>(offset));
            }
        }
    }

    // A C# struct: the setter takes the struct by reference, and is given the one inside the box.
    private delegate void StructSetter<TTarget, in TProperty>(ref TTarget target, TProperty value);

    private sealed class StructPropertyReader<TTarget, TProperty>(string name, StructSetter<TTarget, TProperty> setter, VectorDataReaderBase? reader)
        : PropertyReader(name) where TTarget : struct
    {
        public StructPropertyReader(string name, MethodInfo setMethod) : this(name, setMethod.CreateDelegate<StructSetter<TTarget, TProperty>>(), null)
        {
        }

        public override PropertyReader WithReader(VectorDataReaderBase fieldReader) => new StructPropertyReader<TTarget, TProperty>(Name, setter, fieldReader);

        public override void Read(object target, ulong offset)
        {
            if (reader!.IsValid(offset))
            {
                setter(ref Unsafe.Unbox<TTarget>(target), reader.ReadValidValue<TProperty>(offset));
            }
            else if (typeof(TProperty).IsValueType)
            {
                ThrowNotNullable();
            }
        }
    }

    private sealed class NullableStructPropertyReader<TTarget, TValue>(string name, StructSetter<TTarget, TValue?> setter, VectorDataReaderBase? reader)
        : PropertyReader(name) where TTarget : struct where TValue : struct
    {
        public NullableStructPropertyReader(string name, MethodInfo setMethod) : this(name, setMethod.CreateDelegate<StructSetter<TTarget, TValue?>>(), null)
        {
        }

        public override PropertyReader WithReader(VectorDataReaderBase fieldReader) => new NullableStructPropertyReader<TTarget, TValue>(Name, setter, fieldReader);

        public override void Read(object target, ulong offset)
        {
            if (reader!.IsValid(offset))
            {
                setter(ref Unsafe.Unbox<TTarget>(target), reader.ReadValidValue<TValue>(offset));
            }
        }
    }

    // Where no code can be made at run time: the field is read as an object of the property type (or the type under
    // a nullable one) and set through a boxed setter.
    private sealed class BoxedPropertyReader(string name, PropertyDetails property, VectorDataReaderBase reader) : PropertyReader(name)
    {
        public override PropertyReader WithReader(VectorDataReaderBase fieldReader) => new BoxedPropertyReader(Name, property, fieldReader);

        public override void Read(object target, ulong offset)
        {
            if (reader.IsValid(offset))
            {
                var value = reader.GetValue(offset, property.NullableType ?? property.PropertyType);
                property.Setter(target, value);
            }
            else if (!property.Nullable)
            {
                ThrowNotNullable();
            }
        }
    }

    internal override void Reset(IntPtr vector)
    {
        base.Reset(vector);
        for (int index = 0; index < orderedReaders.Length; index++)
        {
            var childVector = NativeMethods.Vectors.DuckDBStructVectorGetChild(vector, index);
            orderedReaders[index].Reset(childVector);
        }
    }

    public override void Dispose()
    {
        foreach (var reader in orderedReaders)
        {
            reader.Dispose();
        }

        base.Dispose();
    }
}