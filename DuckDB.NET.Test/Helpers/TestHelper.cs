using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DuckDB.NET.Test.Helpers
{
    internal static class TestHelper
    {
        /// <summary>
        /// Generate an IDisposal wrapper for temp files that will automatically delete them.
        /// </summary>
        public static DisposableFile GetDisposableFile(string extension = null, int? index = null, bool create = false)
        {
            var fileBuilder = new StringBuilder(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString()));

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
    }
}
