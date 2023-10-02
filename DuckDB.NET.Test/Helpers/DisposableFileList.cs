using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DuckDB.NET.Test.Helpers;

internal class DisposableFileList : List<DisposableFile>, IDisposable
{
    public DisposableFileList(int fileCount, string extension = null, bool create = false)
    {
        for(int i = 0; i < fileCount; i++)
        {
            Add(DisposableFile.GenerateInTemp(extension, i, create));
        }
    }

    public void Dispose()
    {
        foreach(DisposableFile file in this)
        {
            file.Dispose();
        }

        Clear();
    }
}