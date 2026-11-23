using System.Runtime.CompilerServices;
using DuckDB.NET.Data.Execution;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.DataChunk.Reader;

// The setters of a target type's properties, which do not depend on the column, are in
// StructVectorDataReader.Setters.cs.
internal sealed partial class StructVectorDataReader : VectorDataReaderBase
{
    // The array holds every field by ordinal, for Reset and Dispose. A dictionary, ordered or not, cannot replace it:
    // the fields of an unnamed struct, row(1, 2), all have the empty name, so only the last one stays in it.
    private readonly Dictionary<string, VectorDataReaderBase> fieldReadersByName;
    private readonly VectorDataReaderBase[] fieldReaders;

    // What GetValueCore found out about the type it was asked for last. A type's setters are worked out once per
    // process (SetterCache); this reader pairs them with its fields once per type it is asked for, as a column is
    // read as the same type row after row.
    private Type? planType;
    private StructPlan plan;

    internal unsafe StructVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        using var logicalType = NativeMethods.Vectors.DuckDBVectorGetColumnType(vector);
        var memberCount = NativeMethods.LogicalType.DuckDBStructTypeChildCount(logicalType);
        fieldReadersByName = new Dictionary<string, VectorDataReaderBase>((int)memberCount, StringComparer.OrdinalIgnoreCase);
        fieldReaders = new VectorDataReaderBase[memberCount];

        for (int index = 0; index < memberCount; index++)
        {
            var name = NativeMethods.LogicalType.DuckDBStructTypeChildName(logicalType, index);
            var childVector = NativeMethods.Vectors.DuckDBStructVectorGetChild(vector, index);

            using var childType = NativeMethods.LogicalType.DuckDBStructTypeChildType(logicalType, index);
            var reader = VectorDataReaderFactory.CreateReader(childVector, childType, columnName);
            fieldReadersByName[name] = reader;
            fieldReaders[index] = reader;
        }
    }

    /// <summary>A reader for a STRUCT column of a V2 result: one type parameter per field, named after it.</summary>
    internal unsafe StructVectorDataReader(_duckdb_v2_logical_type* type, string columnName) : base(null, null, DuckDBType.Struct, columnName)
    {
        var memberCount = (int)DuckDBV2LogicalTypes.GetParamCount(type);
        fieldReadersByName = new Dictionary<string, VectorDataReaderBase>(memberCount, StringComparer.OrdinalIgnoreCase);
        fieldReaders = new VectorDataReaderBase[memberCount];

        for (var index = 0; index < memberCount; index++)
        {
            using var childType = DuckDBV2LogicalTypes.GetTypeParam(type, (ulong)index, out var name);
            using var lease = childType.Acquire();

            var reader = VectorDataReaderFactory.CreateReader(lease.Pointer, columnName);
            fieldReadersByName[name] = reader;
            fieldReaders[index] = reader;
        }
    }

    internal override object GetValueCore(ulong offset, Type targetType)
    {
        if (DuckDBType != DuckDBType.Struct)
        {
            return base.GetValueCore(offset, targetType);
        }

        if (targetType != planType)
        {
            plan = CreatePlan(targetType);
            planType = targetType;
        }

        return plan.IsDictionary ? GetDictionary(offset, targetType) : GetObject(offset, targetType);
    }

    // A Dictionary<string, object>, or a class derived from one: every field, read as the column's own type.
    private object GetDictionary(ulong offset, Type targetType)
    {
        var dictionary = (Dictionary<string, object?>)Activator.CreateInstance(targetType)!;

        foreach (var reader in fieldReadersByName)
        {
            var value = reader.Value.IsValid(offset) ? reader.Value.GetValue(offset) : null;
            dictionary.Add(reader.Key, value);
        }

        return dictionary;
    }

    // Any other type: each property that has a field of its name is set. A C# struct is set inside its box, which is
    // what is returned.
    private object GetObject(ulong offset, Type targetType)
    {
        var result = Activator.CreateInstance(targetType)!;

        foreach (var (setter, fieldReader) in plan.Properties)
        {
            setter.Set(result, fieldReader, offset);
        }

        return result;
    }

    // Not inlined, so that GetValueCore does not set up a native-call frame on every row (Reader/DESIGN.md).
    [MethodImpl(MethodImplOptions.NoInlining)]
    private StructPlan CreatePlan(Type targetType)
    {
        if (typeof(Dictionary<string, object?>).IsAssignableFrom(targetType))
        {
            return new StructPlan(IsDictionary: true, Properties: []);
        }

        // A static lambda: one that used targetType allocated a closure and a delegate for every call.
        // CreateTypedSetters returns null where no code can be made at run time.
        var setters = SetterCache.GetOrAdd(targetType, static type => CreateTypedSetters(type) ?? CreateBoxedSetters(type));

        // In the order of the type's properties, each settable property that has a field of the same name. The other
        // properties are left as they are.
        var properties = new List<BoundProperty>(setters.Length);

        foreach (var setter in setters)
        {
            if (fieldReadersByName.TryGetValue(setter.Name, out var fieldReader))
            {
                properties.Add(new BoundProperty(setter, fieldReader));
            }
        }

        return new StructPlan(IsDictionary: false, Properties: [.. properties]);
    }

    // IsDictionary: the target is a Dictionary<string, object> or derives from one. Properties: otherwise, the
    // properties that are set, each with the reader of its field.
    private readonly record struct StructPlan(bool IsDictionary, BoundProperty[] Properties);

    // A property of the target type, with the reader of the field of the same name.
    private readonly record struct BoundProperty(PropertySetter Setter, VectorDataReaderBase FieldReader);

    internal override void Reset(IntPtr vector)
    {
        base.Reset(vector);
        for (int index = 0; index < fieldReaders.Length; index++)
        {
            var childVector = NativeMethods.Vectors.DuckDBStructVectorGetChild(vector, index);
            fieldReaders[index].Reset(childVector);
        }
    }

    internal override unsafe void Reset(_duckdb_v2_vector* vector)
    {
        // The struct is flattened here first, so its fields are indexed by its rows.
        base.Reset(vector);

        for (var index = 0; index < fieldReaders.Length; index++)
        {
            fieldReaders[index].Reset(DuckDBV2Vectors.GetChild(vector, index));
        }
    }

    public override void Dispose()
    {
        foreach (var reader in fieldReaders)
        {
            reader.Dispose();
        }

        base.Dispose();
    }
}