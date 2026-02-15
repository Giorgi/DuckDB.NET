namespace DuckDB.NET.Native;

public partial class NativeMethods
{
	public static partial class Vectors
	{
		[LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_vector_get_column_type")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static partial DuckDBLogicalType DuckDBVectorGetColumnType(IntPtr vector);

		[LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_vector_get_data")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static unsafe partial void* DuckDBVectorGetData(IntPtr vector);

		[LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_vector_get_validity")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static unsafe partial ulong* DuckDBVectorGetValidity(IntPtr vector);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_vector_ensure_validity_writable")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBVectorEnsureValidityWritable(IntPtr vector);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_vector_assign_string_element", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBVectorAssignStringElement(IntPtr vector, ulong index, string value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_vector_assign_string_element_len")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBVectorAssignStringElementLength(IntPtr vector, ulong index, byte* handle, long length);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_list_vector_get_child")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static partial IntPtr DuckDBListVectorGetChild(IntPtr vector);

		[LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_list_vector_get_size")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static partial long DuckDBListVectorGetSize(IntPtr vector);

		[LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_list_vector_reserve")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static partial DuckDBState DuckDBListVectorReserve(IntPtr vector, ulong requiredCapacity);

		[LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_struct_vector_get_child")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static partial IntPtr DuckDBStructVectorGetChild(IntPtr vector, long index);

		[LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_array_vector_get_child")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static partial IntPtr DuckDBArrayVectorGetChild(IntPtr vector);
	}
}
