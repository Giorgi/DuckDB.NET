using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;

namespace DuckDB.NET.Test;

/// <summary>
/// Compares every <see cref="LibraryImportAttribute"/> in the bindings with its prototype in the duckdb.h that the
/// build downloaded next to the native library. A parameter or return type of the wrong width can work by chance on
/// x64 and arm64 (an int passed where DuckDB reads a 64-bit idx_t, or a return value nobody reads), and fail on
/// another ABI: on WebAssembly a call through a mismatched signature traps. Only types whose width is known on both
/// sides are compared: integers, size_t, floating point and void. Pointers, handles, strings, structs and bool are
/// left out.
/// </summary>
public class NativeSignatureTests
{
    private enum Kind { Integer, NativeInteger, Float, Void }

    private readonly record struct Shape(Kind Kind, int Size)
    {
        public override string ToString() => Kind switch
        {
            Kind.Void => "void",
            Kind.NativeInteger => "native-sized integer",
            _ => $"{Size}-byte {Kind.ToString().ToLowerInvariant()}",
        };
    }

    private record Prototype(string Name, string ReturnType, List<string> Parameters);

    [Fact]
    public void EveryLibraryImportMatchesTheWidthsInDuckDBHeader()
    {
        var (prototypes, enums) = ParseHeaders();
        var imports = LibraryImports();

        imports.Should().HaveCountGreaterThan(200, "the bindings declare that many native functions");

        var problems = new List<string>();
        foreach (var (method, entryPoint) in imports)
        {
            if (!prototypes.TryGetValue(entryPoint, out var prototype))
            {
                problems.Add($"{entryPoint}: not declared in duckdb.h");
                continue;
            }

            var parameters = method.GetParameters();
            if (parameters.Length != prototype.Parameters.Count)
            {
                problems.Add($"{entryPoint}: {parameters.Length} parameters in {Describe(method)}, {prototype.Parameters.Count} in duckdb.h");
                continue;
            }

            Compare(problems, entryPoint, "return type", prototype.ReturnType, method.ReturnType, enums);
            for (var i = 0; i < parameters.Length; i++)
            {
                Compare(problems, entryPoint, $"parameter '{parameters[i].Name}'", prototype.Parameters[i], parameters[i].ParameterType, enums);
            }
        }

        Assert.True(problems.Count == 0, $"{problems.Count} declarations do not match duckdb.h:\n" + string.Join("\n", problems));
    }

    private static void Compare(List<string> problems, string entryPoint, string what, string nativeType, Type managedType, HashSet<string> enums)
    {
        var native = NativeShape(nativeType, enums);
        if (native is null)
        {
            return;
        }

        var managed = ManagedShape(managedType);
        if (managed != native)
        {
            problems.Add($"{entryPoint}: {what} is '{nativeType}' ({native}) in duckdb.h but {managedType.Name} ({managed?.ToString() ?? "not a number"}) in the bindings");
        }
    }

    private static Shape? NativeShape(string type, HashSet<string> enums)
    {
        if (type.Contains('*') || type.Contains('['))
        {
            return null;
        }

        var name = Regex.Replace(type, @"\b(const|volatile|struct|enum)\b", "").Trim();
        return name switch
        {
            "void" => new Shape(Kind.Void, 0),
            "idx_t" or "int64_t" or "uint64_t" => new Shape(Kind.Integer, 8),
            "int32_t" or "uint32_t" or "int" or "unsigned" or "unsigned int" or "sel_t" => new Shape(Kind.Integer, 4),
            "int16_t" or "uint16_t" => new Shape(Kind.Integer, 2),
            "int8_t" or "uint8_t" => new Shape(Kind.Integer, 1),
            "size_t" => new Shape(Kind.NativeInteger, 0),
            "float" => new Shape(Kind.Float, 4),
            "double" => new Shape(Kind.Float, 8),
            _ when enums.Contains(name) => new Shape(Kind.Integer, 4),
            _ => null,
        };
    }

    private static Shape? ManagedShape(Type type)
    {
        if (type == typeof(void)) return new Shape(Kind.Void, 0);
        if (type.IsByRef || type.IsPointer) return null;
        if (type.IsEnum) type = Enum.GetUnderlyingType(type);
        if (type == typeof(long) || type == typeof(ulong)) return new Shape(Kind.Integer, 8);
        if (type == typeof(int) || type == typeof(uint)) return new Shape(Kind.Integer, 4);
        if (type == typeof(short) || type == typeof(ushort)) return new Shape(Kind.Integer, 2);
        if (type == typeof(byte) || type == typeof(sbyte)) return new Shape(Kind.Integer, 1);
        if (type == typeof(nint) || type == typeof(nuint)) return new Shape(Kind.NativeInteger, 0);
        if (type == typeof(float)) return new Shape(Kind.Float, 4);
        if (type == typeof(double)) return new Shape(Kind.Float, 8);
        return null;
    }

    private static List<(MethodInfo Method, string EntryPoint)> LibraryImports()
    {
        return typeof(NativeMethods).GetNestedTypes(BindingFlags.Public | BindingFlags.NonPublic)
            .SelectMany(type => type.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.DeclaredOnly))
            .Select(method => (Method: method, Attribute: method.GetCustomAttribute<LibraryImportAttribute>()))
            .Where(import => import.Attribute is not null)
            .Select(import => (import.Method, import.Attribute.EntryPoint ?? import.Method.Name))
            .ToList();
    }

    private static string Describe(MethodInfo method) => $"{method.DeclaringType?.Name}.{method.Name}";

    private static (Dictionary<string, Prototype> Prototypes, HashSet<string> Enums) ParseHeaders()
    {
        var text = string.Join("\n", HeaderPaths().Select(File.ReadAllText));
        text = Regex.Replace(text, @"/\*.*?\*/", " ", RegexOptions.Singleline);
        text = Regex.Replace(text, @"//[^\n]*", " ");

        var enums = Regex.Matches(text, @"typedef\s+enum\s*\w*\s*\{[^}]*\}\s*(\w+)\s*;")
            .Select(m => m.Groups[1].Value)
            .ToHashSet();

        var prototypes = new Dictionary<string, Prototype>();
        foreach (Match match in Regex.Matches(text, @"DUCKDB_C_API\s+([^;{}()]*?)\b(duckdb_\w+)\s*\(([^;{}]*?)\)\s*;"))
        {
            var returnType = Regex.Replace(match.Groups[1].Value, @"\s+", " ").Trim();
            var parameterList = Regex.Replace(match.Groups[3].Value, @"\s+", " ").Trim();
            var parameters = parameterList is "" or "void"
                ? new List<string>()
                : parameterList.Split(',').Select(ParameterType).ToList();
            prototypes[match.Groups[2].Value] = new Prototype(match.Groups[2].Value, returnType, parameters);
        }

        prototypes.Should().HaveCountGreaterThan(400, "duckdb.h declares that many functions");
        return (prototypes, enums);
    }

    // "const void *data" -> "const void *", "idx_t length" -> "idx_t"
    private static string ParameterType(string parameter)
    {
        var trimmed = parameter.Trim();
        var name = Regex.Match(trimmed, @"(\w+)\s*(\[\s*\])?$");
        var type = name.Success ? trimmed[..name.Index] + (name.Groups[2].Success ? "*" : "") : trimmed;
        return type.Trim();
    }

    private static IEnumerable<string> HeaderPaths()
    {
        // The build downloads duckdb.h with the native library into DuckDB.NET.Bindings/obj/runtimes/<rid>/native.
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            var runtimes = Path.Combine(directory.FullName, "DuckDB.NET.Bindings", "obj", "runtimes");
            if (!Directory.Exists(runtimes))
            {
                continue;
            }

            var header = Directory.GetDirectories(runtimes)
                .Select(rid => Path.Combine(rid, "native"))
                .FirstOrDefault(native => File.Exists(Path.Combine(native, "duckdb.h")));
            if (header is not null)
            {
                return new[] { "duckdb.h", "duckdb_static_extension.h" }
                    .Select(file => Path.Combine(header, file))
                    .Where(File.Exists);
            }
        }

        throw new FileNotFoundException("duckdb.h was not found under DuckDB.NET.Bindings/obj/runtimes; the build downloads it with the native library");
    }
}
