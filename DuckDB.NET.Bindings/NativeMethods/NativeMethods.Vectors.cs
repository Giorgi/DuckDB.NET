namespace DuckDB.NET.Native;

public partial class NativeMethods
{
	public static partial class Vectors
	{
		// Maybe [SuppressGCTransition]: new LogicalType — one small allocation
		[LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_vector_get_column_type")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static partial DuckDBLogicalType DuckDBVectorGetColumnType(IntPtr vector);

		[SuppressGCTransition]
		[LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_vector_get_data")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static unsafe partial void* DuckDBVectorGetData(IntPtr vector);

		[SuppressGCTransition]
		[LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_vector_get_validity")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static unsafe partial ulong* DuckDBVectorGetValidity(IntPtr vector);

        // Maybe [SuppressGCTransition]: may copy-on-write allocate validity bitmap if shared
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_vector_ensure_validity_writable")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBVectorEnsureValidityWritable(IntPtr vector);

        // Maybe [SuppressGCTransition]: UTF-8 validation + StringVector::AddStringOrBlob — allocation for long strings
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_vector_assign_string_element", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBVectorAssignStringElement(IntPtr vector, ulong index, string value);

        // Maybe [SuppressGCTransition]: UTF-8 validation (VARCHAR only) + allocation for long strings
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_vector_assign_string_element_len")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBVectorAssignStringElementLength(IntPtr vector, ulong index, byte* handle, long length);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_list_vector_get_child")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static partial IntPtr DuckDBListVectorGetChild(IntPtr vector);

		[SuppressGCTransition]
		[LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_list_vector_get_size")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static partial long DuckDBListVectorGetSize(IntPtr vector);

		// Maybe [SuppressGCTransition]: may reallocate child vector buffer
		[LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_list_vector_reserve")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static partial DuckDBState DuckDBListVectorReserve(IntPtr vector, ulong requiredCapacity);

		[SuppressGCTransition]
		[LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_struct_vector_get_child")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static partial IntPtr DuckDBStructVectorGetChild(IntPtr vector, long index);

		[SuppressGCTransition]
		[LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_array_vector_get_child")]
		[UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
		public static partial IntPtr DuckDBArrayVectorGetChild(IntPtr vector);
	}
}
