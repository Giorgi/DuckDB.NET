using DuckDB.NET.Data.PreparedStatement;

namespace DuckDB.NET.Data;

// The statements prepared by DuckDBCommand.Prepare(), kept by the connection until it closes. See DESIGN.md in
// PreparedStatement.
partial class DuckDBConnection
{
    private readonly object preparedStatementsLock = new();
    private Dictionary<string, PreparedStatementEntry>? preparedStatements;
    private List<PreparedStatementEntry>? retiredStatements;

    /// <summary>
    /// Prepares the command text anew and makes it the statement that commands with this text on this connection use.
    /// The statement it replaces is destroyed; commands that still hold it take the new one on their next execution.
    /// Null for a command of several statements, which is prepared on each execution instead. When preparing fails,
    /// the kept statement is dropped too: it no longer matches the database (a view it reads was dropped).
    /// </summary>
    internal PreparedStatementEntry? PrepareStatement(string commandText)
    {
        PreparedStatementEntry? entry;
        try
        {
            entry = PreparedStatement.PreparedStatement.PrepareSingle(NativeConnection, commandText);
        }
        catch
        {
            lock (preparedStatementsLock)
            {
                if (preparedStatements is not null && preparedStatements.Remove(commandText, out var stale))
                {
                    Retire(stale);
                }
            }

            throw;
        }

        lock (preparedStatementsLock)
        {
            preparedStatements ??= new Dictionary<string, PreparedStatementEntry>(StringComparer.Ordinal);

            if (preparedStatements.Remove(commandText, out var replaced))
            {
                Retire(replaced);
            }

            if (entry is not null)
            {
                preparedStatements[commandText] = entry;
            }
        }

        return entry;
    }

    /// <summary>
    /// The statement kept for the command text, prepared now if there is none. Entries are kept until the connection
    /// closes.
    /// </summary>
    internal PreparedStatementEntry? GetOrPrepareStatement(string commandText)
    {
        lock (preparedStatementsLock)
        {
            if (preparedStatements is not null && preparedStatements.TryGetValue(commandText, out var entry) && !entry.IsStale)
            {
                return entry;
            }
        }

        return PrepareStatement(commandText);
    }

    private void Retire(PreparedStatementEntry entry)
    {
        entry.MarkRetired();

        // An execution on another thread may hold it; then that execution destroys it when it gives it back, and the
        // connection keeps it until then, to destroy it when it closes if that comes first.
        if (entry.TryRent())
        {
            entry.Dispose();
        }
        else
        {
            retiredStatements ??= [];
            retiredStatements.RemoveAll(retired => retired.IsDisposed);
            retiredStatements.Add(entry);
        }
    }

    private void DisposePreparedStatements()
    {
        lock (preparedStatementsLock)
        {
            if (preparedStatements is not null)
            {
                foreach (var entry in preparedStatements.Values)
                {
                    entry.Dispose();
                }
            }

            foreach (var entry in retiredStatements ?? [])
            {
                entry.Dispose();
            }

            preparedStatements = null;
            retiredStatements = null;
        }
    }
}
