using System;
using System.IO;

namespace DuckDB.NET.Test.Helpers
{
    sealed class DisposableFile : IDisposable
    {
        private bool disposed;

        public string FileName { get; private set; }

        public DisposableFile(string filename)
        {
            FileName = filename;
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
}
