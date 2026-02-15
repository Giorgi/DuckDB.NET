namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#logical-type-interface
    public static partial class LogicalType
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_logical_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBLogicalType DuckDBCreateLogicalType(DuckDBType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_decimal_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBLogicalType DuckDBCreateDecimalType(byte width, byte scale);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_type_id")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBType DuckDBGetTypeId(DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_decimal_width")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial byte DuckDBDecimalWidth(DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_decimal_scale")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial byte DuckDBDecimalScale(DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_decimal_internal_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBType DuckDBDecimalInternalType(DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_enum_internal_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBType DuckDBEnumInternalType(DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_enum_dictionary_size")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial uint DuckDBEnumDictionarySize(DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_enum_dictionary_value")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalUsing(typeof(DuckDBCallerOwnedStringMarshaller))]
        public static partial string DuckDBEnumDictionaryValue(DuckDBLogicalType type, long index);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_list_type_child_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBLogicalType DuckDBListTypeChildType(DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_array_type_child_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBLogicalType DuckDBArrayTypeChildType(DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_map_type_key_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBLogicalType DuckDBMapTypeKeyType(DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_map_type_value_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBLogicalType DuckDBMapTypeValueType(DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_struct_type_child_count")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial long DuckDBStructTypeChildCount(DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_struct_type_child_name")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalUsing(typeof(DuckDBCallerOwnedStringMarshaller))]
        public static partial string DuckDBStructTypeChildName(DuckDBLogicalType type, long index);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_struct_type_child_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBLogicalType DuckDBStructTypeChildType(DuckDBLogicalType type, long index);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_logical_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyLogicalType(ref IntPtr type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_array_type_array_size")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial long DuckDBArrayVectorGetSize(DuckDBLogicalType logicalType);
    }
}
