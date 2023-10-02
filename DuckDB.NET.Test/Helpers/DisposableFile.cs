using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace DuckDB.NET.Test.Helpers;

sealed class DisposableFile : IDisposable
{
    private bool disposed;

    public string ConnectionString { get; private set; }
    public string FileName { get; private set; }

    public DisposableFile(string filename)
    {
        FileName = filename;
        ConnectionString = $"DataSource={FileName}";
    }

    public static DisposableFile GenerateInTemp(string extension = null, int? index = null, bool create = false)
    {
        var fileBuilder = new StringBuilder(Path.Combine(Path.GetTempPath(), "Temp File-" + Guid.NewGuid().ToString()));

        if (index != null)
        {
            fileBuilder.Append("_");
            fileBuilder.Append(index.ToString());
        }

        if (extension != null)
        {
            fileBuilder.Append(".");
            fileBuilder.Append(extension);
        }

        var filename = fileBuilder.ToString();

        if (create)
        {
            File.Create(filename);
        }

        return new DisposableFile(filename);
    }

    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        File.Delete(FileName);

        disposed = true;

        GC.SuppressFinalize(this);
    }

    ~DisposableFile()
    {
        Dispose();
    }
}