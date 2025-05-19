using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#logical-type-interface
    public static class LogicalType
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_logical_type")]
        public static extern DuckDBLogicalType DuckDBCreateLogicalType(DuckDBType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_decimal_type")]
        public static extern DuckDBLogicalType DuckDBCreateDecimalType(byte width, byte scale);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_type_id")]
        public static extern DuckDBType DuckDBGetTypeId(DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_decimal_width")]
        public static extern byte DuckDBDecimalWidth(DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_decimal_scale")]
        public static extern byte DuckDBDecimalScale(DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_decimal_internal_type")]
        public static extern DuckDBType DuckDBDecimalInternalType(DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_enum_internal_type")]
        public static extern DuckDBType DuckDBEnumInternalType(DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_enum_dictionary_size")]
        public static extern uint DuckDBEnumDictionarySize(DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_enum_dictionary_value")]
        public static extern IntPtr DuckDBEnumDictionaryValue(DuckDBLogicalType type, long index);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_list_type_child_type")]
        public static extern DuckDBLogicalType DuckDBListTypeChildType(DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_array_type_child_type")]
        public static extern DuckDBLogicalType DuckDBArrayTypeChildType(DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_map_type_key_type")]
        public static extern DuckDBLogicalType DuckDBMapTypeKeyType(DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_map_type_value_type")]
        public static extern DuckDBLogicalType DuckDBMapTypeValueType(DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_struct_type_child_count")]
        public static extern long DuckDBStructTypeChildCount(DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_struct_type_child_name")]
        public static extern IntPtr DuckDBStructTypeChildName(DuckDBLogicalType type, long index);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_struct_type_child_type")]
        public static extern DuckDBLogicalType DuckDBStructTypeChildType(DuckDBLogicalType type, long index);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_logical_type")]
        public static extern void DuckDBDestroyLogicalType(ref IntPtr type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_array_type_array_size")]
        public static extern long DuckDBArrayVectorGetSize(DuckDBLogicalType logicalType);
    }
}