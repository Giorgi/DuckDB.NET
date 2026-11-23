using DuckDB.NET.Data.Connection;
using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using DuckDB.NET.Data.Execution;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data;

partial class DuckDBConnection
{
    public void RegisterScalarFunction<TResult>(string name, Action<IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        RegisterScalarMethod(name, (_, w, index) => action(w, index), typeof(TResult), varargs: false, options);
    }

    public void RegisterScalarFunction<T, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null, bool @params = false)
    {
        RegisterScalarMethod(name, action, typeof(TResult), @params, options, typeof(T));
    }

    public void RegisterScalarFunction<T1, T2, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        RegisterScalarMethod(name, action, typeof(TResult), varargs: false, options,
            typeof(T1), typeof(T2));
    }

    public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        RegisterScalarMethod(name, action, typeof(TResult), varargs: false, options,
            typeof(T1), typeof(T2), typeof(T3));
    }

    public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        RegisterScalarMethod(name, action, typeof(TResult), varargs: false, options,
            typeof(T1), typeof(T2),
            typeof(T3), typeof(T4));
    }

    // Every scalar function is registered here. The action gets the readers and the writer as the classes they are.
    // The public methods above take an action over the interfaces, which is an action over the classes as well.
    // The typed functions in DuckDBConnectionScalarFunctionExtensions call this method with the classes: through an
    // interface, GetValue<T> and WriteValue<T> are looked up at run time on every call.
    internal unsafe void RegisterScalarMethod(string name, Action<VectorDataReaderBase[], VectorDataWriterBase, ulong> action, Type returnType,
                                              bool varargs, ScalarFunctionOptions? options, params Type[] parameterTypes)
    {
        if (varargs && parameterTypes.Length != 1)
        {
            throw new InvalidOperationException("Cannot use params with multiple parameters");
        }

        var connection = V2Connection;
        using var connectionLease = connection.Acquire();
        _duckdb_v2_error_info* error = null;

        _duckdb_v2_factory* factory;
        NativeMethodsV2.duckdb_v2_connection_get_factory(connectionLease.Pointer, &factory, &error).ThrowIfFailed(ref error);

        // The types are made before the function, so a type that cannot be mapped leaves nothing to destroy but them.
        var factoryHandle = (IntPtr)factory;
        var info = new ScalarFunctionInfo(DuckDBV2Types.ForClrType(factoryHandle, returnType),
                                          Array.ConvertAll(parameterTypes, type => DuckDBV2Types.ForClrType(factoryHandle, type)), action);

        _duckdb_v2_scalar_function* function = null;

        try
        {
            NativeMethodsV2.duckdb_v2_scalar_function_create(factory, &function, &error).ThrowIfFailed(ref error);

            using (var text = DuckDBV2Utf8.FromManaged(name))
            {
                fixed (byte* pointer = text.Bytes)
                {
                    NativeMethodsV2.duckdb_v2_scalar_function_set_name(function, (sbyte*)pointer, (ulong)text.Length, &error).ThrowIfFailed(ref error);
                }
            }

            _duckdb_v2_function_signature* signature;
            NativeMethodsV2.duckdb_v2_scalar_function_get_signature(function, &signature, &error).ThrowIfFailed(ref error);

            // Positional-only parameters: their names are invisible to SQL, but two may not share one.
            for (var index = 0; index < info.ParameterTypes.Length; index++)
            {
                using var typeLease = info.ParameterTypes[index].Acquire();
                using var parameterName = DuckDBV2Utf8.FromManaged($"arg{index}");
                var kind = varargs ? DUCKDB_V2_FUNCTION_PARAMETER_KIND.POSITIONAL_VARIADIC : DUCKDB_V2_FUNCTION_PARAMETER_KIND.POSITIONAL_ONLY;

                fixed (byte* pointer = parameterName.Bytes)
                {
                    NativeMethodsV2.duckdb_v2_function_signature_add_parameter(signature, (sbyte*)pointer, (ulong)parameterName.Length, typeLease.Pointer, null, kind, &error)
                                   .ThrowIfFailed(ref error);
                }
            }

            using (var returnLease = info.ReturnType.Acquire())
            {
                NativeMethodsV2.duckdb_v2_function_signature_set_return_type(signature, returnLease.Pointer, &error).ThrowIfFailed(ref error);
            }

            // Functions with parameters default to pure; parameterless functions (e.g. random()) default to volatile
            var defaultPure = parameterTypes.Length > 0;
            if (!(options?.IsPureFunction ?? defaultPure))
            {
                NativeMethodsV2.duckdb_v2_scalar_function_set_property(function, DUCKDB_V2_FUNCTION_PROPERTY_KEY.DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
                                                                       DUCKDB_V2_FUNCTION_PROPERTY_VALUE.DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE, &error).ThrowIfFailed(ref error);
            }

            if (options?.HandlesNulls == true)
            {
                NativeMethodsV2.duckdb_v2_scalar_function_set_property(function, DUCKDB_V2_FUNCTION_PROPERTY_KEY.DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING,
                                                                       DUCKDB_V2_FUNCTION_PROPERTY_VALUE.DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_SPECIAL, &error).ThrowIfFailed(ref error);
            }

            NativeMethodsV2.duckdb_v2_scalar_function_set_bind_callback(function, &ScalarFunctionBind, &error).ThrowIfFailed(ref error);
            NativeMethodsV2.duckdb_v2_scalar_function_set_exec_callback(function, &ScalarFunctionCallback, &error).ThrowIfFailed(ref error);

            // From here DuckDB owns the info: it destroys it with the function, or with the registered function.
            var userData = new duckdb_v2_opaque { ptr = (void*)GCHandle.ToIntPtr(GCHandle.Alloc(info)), destroy = &DestroyHandle };
            var userDataCode = NativeMethodsV2.duckdb_v2_scalar_function_set_user_data(function, &userData, &error);
            if (userDataCode != DUCKDB_V2_ERROR.NONE)
            {
                GCHandle.FromIntPtr((IntPtr)userData.ptr).Free();
                userDataCode.ThrowIfFailed(ref error);
            }
            info = null;

            var code = NativeMethodsV2.duckdb_v2_connection_register_scalar_function(connectionLease.Pointer, function, &error);
            if (code != DUCKDB_V2_ERROR.NONE)
            {
                try
                {
                    code.ThrowIfFailed(ref error);
                }
                catch (DuckDBException exception)
                {
                    throw new InvalidOperationException($"Error registering user defined scalar function: {name}", exception);
                }
            }
        }
        finally
        {
            info?.Dispose();
            NativeMethodsV2.duckdb_v2_scalar_function_destroy(&function);
        }
    }

    // Binding runs on the thread of the statement's connection (DuckDBV2CallbackScope). The connection becomes the bind
    // data, so the exec callbacks, on DuckDB's worker threads, can leave an exception with it. DuckDB shares the bind
    // data between the copies it makes of a bound expression, for a filter in WHERE or HAVING for example.
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void ScalarFunctionBind(_duckdb_v2_function_bind_info* info, _duckdb_v2_scalar_function_bind_info* result,
                                                  _duckdb_v2_context* context, _duckdb_v2_error_info** err)
    {
        try
        {
            // Without a connection the function still runs; only the InnerException is lost.
            if (DuckDBV2CallbackScope.Current is not { } connection)
            {
                return;
            }

            var bindData = new duckdb_v2_opaque { ptr = (void*)GCHandle.ToIntPtr(GCHandle.Alloc(connection)), destroy = &FreeHandle };
            if (NativeMethodsV2.duckdb_v2_scalar_function_bind_set_bind_data(result, &bindData, null) != DUCKDB_V2_ERROR.NONE)
            {
                GCHandle.FromIntPtr((IntPtr)bindData.ptr).Free();
            }
        }
        catch
        {
            // An exception must not leave a callback; the function runs without the bind data.
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void ScalarFunctionCallback(_duckdb_v2_scalar_function_exec_info* info, _duckdb_v2_context* context, _duckdb_v2_error_info** err)
    {
        VectorDataReaderBase[] readers = [];
        VectorDataWriterBase? writer = null;

        try
        {
            void* userData;
            ThrowIfFailed(NativeMethodsV2.duckdb_v2_scalar_function_exec_get_user_data(info, &userData, null));

            if (userData == null || GCHandle.FromIntPtr((IntPtr)userData).Target is not ScalarFunctionInfo functionInfo)
            {
                throw new InvalidOperationException("User defined scalar function execution failed. Function extra info is null");
            }

            ulong rowCount, fixedCount, variadicCount, namedCount, namedVariadicCount;
            ThrowIfFailed(NativeMethodsV2.duckdb_v2_scalar_function_exec_get_row_count(info, &rowCount, null));
            ThrowIfFailed(NativeMethodsV2.duckdb_v2_scalar_function_exec_get_arg_count(info, &fixedCount, &variadicCount, &namedCount, &namedVariadicCount, null));

            // DuckDB casts every argument to its parameter's type; a params function has one type for them all.
            readers = new VectorDataReaderBase[fixedCount + variadicCount];

            for (var index = 0; index < readers.Length; index++)
            {
                _duckdb_v2_vector* vector;
                ThrowIfFailed(NativeMethodsV2.duckdb_v2_scalar_function_exec_get_arg(info, (ulong)index, &vector, null));

                var parameter = Math.Min(index, functionInfo.ParameterTypes.Length - 1);

                if (functionInfo.IsAny[parameter])
                {
                    // DuckDB does not cast an argument of an ANY parameter: it is read as the type it has.
                    _duckdb_v2_logical_type* argumentType;
                    ThrowIfFailed(NativeMethodsV2.duckdb_v2_vector_get_logical_type(vector, &argumentType, null));
                    using var owned = new DuckDBV2LogicalType(argumentType);
                    using var argumentLease = owned.Acquire();
                    readers[index] = VectorDataReaderFactory.CreateReader(argumentLease.Pointer, "");
                }
                else
                {
                    using var typeLease = functionInfo.ParameterTypes[parameter].Acquire();
                    readers[index] = VectorDataReaderFactory.CreateReader(typeLease.Pointer, "");
                }

                readers[index].Reset(vector);
            }

            _duckdb_v2_vector* output;
            ThrowIfFailed(NativeMethodsV2.duckdb_v2_scalar_function_exec_get_result(info, &output, null));

            using (var returnLease = functionInfo.ReturnType.Acquire())
            {
                writer = VectorDataWriterFactory.CreateWriter(output, returnLease.Pointer);
            }

            functionInfo.Action(readers, writer, rowCount);
        }
        catch (Exception ex)
        {
            try
            {
                void* bindData;
                if (NativeMethodsV2.duckdb_v2_scalar_function_exec_get_bind_data(info, &bindData, null) == DUCKDB_V2_ERROR.NONE && bindData != null &&
                    GCHandle.FromIntPtr((IntPtr)bindData).Target is DuckDBV2Connection connection)
                {
                    connection.CallbackException = ex;
                }
            }
            catch
            {
                // Without the connection we still report the error message
            }

            SetError(*err, ex.Message);
        }
        finally
        {
            foreach (var reader in readers)
            {
                reader?.Dispose();
            }

            writer?.Dispose();
        }
    }

    private static void ThrowIfFailed(DUCKDB_V2_ERROR code)
    {
        if (code != DUCKDB_V2_ERROR.NONE)
        {
            throw new InvalidOperationException($"User defined scalar function execution failed: {code}");
        }
    }

    private static unsafe void SetError(_duckdb_v2_error_info* err, string message)
    {
        using var text = DuckDBV2Utf8.FromManaged(message);

        fixed (byte* pointer = text.Bytes)
        {
            NativeMethodsV2.duckdb_v2_error_info_set_code(err, DUCKDB_V2_ERROR.INPUT_INVALID);
            NativeMethodsV2.duckdb_v2_error_info_set_text(err, (sbyte*)pointer, (ulong)text.Length);
        }
    }

    // The function's info, disposed with it.
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void DestroyHandle(void* pointer) => ((IntPtr)pointer).FreeHandle();

    // The bind data: the connection, which only the handle is freed for.
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void FreeHandle(void* pointer) => GCHandle.FromIntPtr((IntPtr)pointer).Free();
}
