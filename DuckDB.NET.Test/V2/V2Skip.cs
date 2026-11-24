namespace DuckDB.NET.Test;

// Why a test is skipped on the C API V2 (docs/V2-MIGRATION.md). Each goes when what it names is migrated or decided.
internal static class V2Skip
{
    public const string Materialized = "Materialized results (UseStreamingMode = false) are not supported on the C API V2 yet (docs/V2-MIGRATION.md, stage 2).";

    public const string ArrowAppender = "DuckDBArrowAppender is not migrated to the C API V2 yet (docs/V2-MIGRATION.md, stage 7).";

    public const string TwoThreadsOnOneConnection = "On the C API V2 a connection has one open result, so two threads executing on one connection collide; " +
                                                    "whether to serialize them is open (docs/V2-MIGRATION.md, stage 3).";
}
