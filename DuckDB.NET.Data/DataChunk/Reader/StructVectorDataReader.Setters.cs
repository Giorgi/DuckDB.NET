using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Reader;

// How a field is read into a property of the target type. Nothing here depends on the column: the setters of a type
// are made once per process and shared by every reader.
internal sealed partial class StructVectorDataReader
{
    // A typed setter per settable property where code can be made at run time, a boxed one where it cannot.
    private static readonly ConcurrentDictionary<Type, PropertySetter[]> SetterCache = new();

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

    // A setter per property, generic over the type that declares the property and the type of the property, so that a
    // field is read with ReadValidValue (the conversion of GetFieldValue<T> on a column) and given to the set method
    // without a box. The set method is called through a delegate made from it; nothing is compiled.
    // Null where no code can be made at run time: the types are the caller's, so none can be named in advance.
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(ClassPropertySetter<,>))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(NullableClassPropertySetter<,>))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(StructPropertySetter<,>))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(NullableStructPropertySetter<,>))]
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "As IL2076, reported by the .NET 8 analyzer.")]
    [UnconditionalSuppressMessage("Trimming", "IL2076", Justification = "The new() constraint that 'where T : struct' implies is met by every value type, and the setters' constructors are kept by the DynamicDependency attributes. The MakeGenericType calls run only where dynamic code is supported.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "The MakeGenericType calls run only where RuntimeFeature.IsDynamicCodeSupported is true, tested in this method; the .NET 8 analyzer does not recognise that guard.")]
    private static PropertySetter[]? CreateTypedSetters(Type type)
    {
        if (!RuntimeFeature.IsDynamicCodeSupported)
        {
            return null;
        }

        // By name, as the boxed setters are: two properties of the same name fail in the same way.
        var setters = new Dictionary<string, PropertySetter>();

        foreach (var property in SettableProperties(type))
        {
            var setMethod = property.SetMethod!;
            var declaringType = setMethod.DeclaringType!;
            var underlyingType = Nullable.GetUnderlyingType(property.PropertyType);

            var setterType = (declaringType.IsValueType, underlyingType != null) switch
            {
                (false, false) => typeof(ClassPropertySetter<,>).MakeGenericType(declaringType, property.PropertyType),
                (false, true) => typeof(NullableClassPropertySetter<,>).MakeGenericType(declaringType, underlyingType!),
                (true, false) => typeof(StructPropertySetter<,>).MakeGenericType(declaringType, property.PropertyType),
                (true, true) => typeof(NullableStructPropertySetter<,>).MakeGenericType(declaringType, underlyingType!)
            };

            // CreateDelegate in the setter's constructor throws for a static or indexed set method, as Expression.Call
            // does on the boxed path.
            var setter = Activator.CreateInstance(setterType, BindingFlags.Public | BindingFlags.Instance | BindingFlags.DoNotWrapExceptions, null, [property.Name, setMethod], null);
            setters.Add(property.Name, (PropertySetter)setter!);
        }

        return [.. setters.Values];
    }

    // Where no code can be made at run time: a boxed setter per property.
    private static PropertySetter[] CreateBoxedSetters(Type type)
    {
        // By name, as the typed setters are: two properties of the same name fail in the same way.
        var setters = new Dictionary<string, PropertySetter>();

        foreach (var property in SettableProperties(type))
        {
            var nullable = property.PropertyType.AllowsNullValue(out _, out var underlyingType);

            var instanceParam = Expression.Parameter(typeof(object));
            var argumentParam = Expression.Parameter(typeof(object));

            // For a C# struct, a compiled delegate would unbox it and set a copy; this path runs only without dynamic
            // code, where Compile interprets the expression and sets the struct inside the box.
            var setValue = Expression.Lambda<Action<object, object>>(
                Expression.Call(Expression.Convert(instanceParam, type), property.SetMethod!, Expression.Convert(argumentParam, property.PropertyType)),
                instanceParam, argumentParam
            ).Compile();

            setters.Add(property.Name, new BoxedPropertySetter(property.Name, underlyingType ?? property.PropertyType, nullable, setValue));
        }

        return [.. setters.Values];
    }

    // Reads one field of a row and sets one property of the target with it. A NULL field leaves a property that can
    // hold null as it is, and is refused for one that cannot.
    //
    // The typed setters are named after the type that declares the property (Class or Struct) and whether the property
    // is of type TValue? (Nullable). Their constructors are called through Activator, in CreateTypedSetters.
    private abstract class PropertySetter(string name)
    {
        public string Name { get; } = name;

        public abstract void Set(object target, VectorDataReaderBase fieldReader, ulong offset);

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        protected void ThrowNotNullable() => throw new InvalidCastException($"Property '{Name}' is not nullable but struct contains null value");
    }

    // The set method of a C# struct's property, taking the struct by reference: it is given the one inside the box.
    private delegate void RefSetMethod<TTarget, in TProperty>(ref TTarget target, TProperty value);

    private sealed class ClassPropertySetter<TTarget, TProperty>(string name, MethodInfo setMethod)
        : PropertySetter(name) where TTarget : class
    {
        private readonly Action<TTarget, TProperty> setValue = setMethod.CreateDelegate<Action<TTarget, TProperty>>();

        public override void Set(object target, VectorDataReaderBase fieldReader, ulong offset)
        {
            if (fieldReader.IsValid(offset))
            {
                setValue(Unsafe.As<TTarget>(target), fieldReader.ReadValidValue<TProperty>(offset));
            }
            else if (typeof(TProperty).IsValueType)
            {
                ThrowNotNullable();
            }
        }
    }

    private sealed class NullableClassPropertySetter<TTarget, TValue>(string name, MethodInfo setMethod)
        : PropertySetter(name) where TTarget : class where TValue : struct
    {
        private readonly Action<TTarget, TValue?> setValue = setMethod.CreateDelegate<Action<TTarget, TValue?>>();

        public override void Set(object target, VectorDataReaderBase fieldReader, ulong offset)
        {
            if (fieldReader.IsValid(offset))
            {
                setValue(Unsafe.As<TTarget>(target), fieldReader.ReadValidValue<TValue>(offset));
            }
        }
    }

    private sealed class StructPropertySetter<TTarget, TProperty>(string name, MethodInfo setMethod)
        : PropertySetter(name) where TTarget : struct
    {
        private readonly RefSetMethod<TTarget, TProperty> setValue = setMethod.CreateDelegate<RefSetMethod<TTarget, TProperty>>();

        public override void Set(object target, VectorDataReaderBase fieldReader, ulong offset)
        {
            if (fieldReader.IsValid(offset))
            {
                setValue(ref Unsafe.Unbox<TTarget>(target), fieldReader.ReadValidValue<TProperty>(offset));
            }
            else if (typeof(TProperty).IsValueType)
            {
                ThrowNotNullable();
            }
        }
    }

    private sealed class NullableStructPropertySetter<TTarget, TValue>(string name, MethodInfo setMethod)
        : PropertySetter(name) where TTarget : struct where TValue : struct
    {
        private readonly RefSetMethod<TTarget, TValue?> setValue = setMethod.CreateDelegate<RefSetMethod<TTarget, TValue?>>();

        public override void Set(object target, VectorDataReaderBase fieldReader, ulong offset)
        {
            if (fieldReader.IsValid(offset))
            {
                setValue(ref Unsafe.Unbox<TTarget>(target), fieldReader.ReadValidValue<TValue>(offset));
            }
        }
    }

    // Where no code can be made at run time: the field is read as an object of readType (the property type, or the
    // type under a nullable one) and set through a boxed delegate.
    private sealed class BoxedPropertySetter(string name, Type readType, bool nullable, Action<object, object> setValue) : PropertySetter(name)
    {
        public override void Set(object target, VectorDataReaderBase fieldReader, ulong offset)
        {
            if (fieldReader.IsValid(offset))
            {
                setValue(target, fieldReader.GetValue(offset, readType));
            }
            else if (!nullable)
            {
                ThrowNotNullable();
            }
        }
    }
}