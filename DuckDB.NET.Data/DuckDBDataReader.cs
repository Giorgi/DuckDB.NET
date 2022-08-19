using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Numerics;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data
{
    public class DuckDBDataReader : DbDataReader
    {
        private readonly DuckDbCommand command;
        private readonly CommandBehavior behavior;

        private readonly DuckDBResult queryResult;

        private int currentRow = -1;
        private bool closed = false;

        internal DuckDBDataReader(DuckDbCommand command, DuckDBResult queryResult, CommandBehavior behavior)
        {
            this.command = command;
            this.behavior = behavior;
            this.queryResult = queryResult;

            HasRows = NativeMethods.Query.DuckDBRowCount(queryResult) > 0;
            FieldCount = (int)NativeMethods.Query.DuckDBColumnCount(queryResult);
        }

        public override bool GetBoolean(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueBoolean(queryResult, ordinal, currentRow);
        }

        public override byte GetByte(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override char GetChar(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            return NativeMethods.Query.DuckDBColumnType(queryResult, ordinal).ToString();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            var text = GetString(ordinal);
            return DateTime.Parse(text, null, DateTimeStyles.RoundtripKind);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return decimal.Parse(GetString(ordinal), CultureInfo.InvariantCulture);
        }

        public override double GetDouble(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueDouble(queryResult, ordinal, currentRow);
        }

        public override Type GetFieldType(int ordinal)
        {
            return NativeMethods.Query.DuckDBColumnType(queryResult, ordinal) switch
            {
                DuckDBType.DuckdbTypeInvalid => throw new DuckDBException("Invalid type"),
                DuckDBType.DuckdbTypeBoolean => typeof(bool),
                DuckDBType.DuckdbTypeSmallInt => typeof(short),
                DuckDBType.DuckdbTypeInteger => typeof(int),
                DuckDBType.DuckdbTypeBigInt => typeof(long),
                DuckDBType.DuckdbTypeFloat => typeof(float),
                DuckDBType.DuckdbTypeDouble => typeof(double),
                DuckDBType.DuckdbTypeTimestamp => typeof(DateTime),
                DuckDBType.DuckdbTypeDate => typeof(DateTime),
                DuckDBType.DuckdbTypeTime => typeof(DateTime),
                DuckDBType.DuckdbTypeInterval => throw new NotImplementedException(),
                DuckDBType.DuckdbTypeHugeInt => typeof(BigInteger),
                DuckDBType.DuckdbTypeVarchar => typeof(string),
                DuckDBType.DuckdbTypeDecimal => typeof(Decimal),
                var typ => throw new ArgumentException($"Unrecognised type {typ} in column {ordinal+1}")
            };
        }

        public override float GetFloat(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueFloat(queryResult, ordinal, currentRow);
        }

        public override Guid GetGuid(int ordinal)
        {
            return new Guid(GetString(ordinal));
        }

        public override short GetInt16(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueInt16(queryResult, ordinal, currentRow);
        }

        public override int GetInt32(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueInt32(queryResult, ordinal, currentRow);
        }

        public override long GetInt64(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueInt64(queryResult, ordinal, currentRow);
        }

        public BigInteger GetBigInteger(int ordinal)
        {
            return BigInteger.Parse(GetString(ordinal));
        }

        public override string GetName(int ordinal)
        {
            return NativeMethods.Query.DuckDBColumnName(queryResult, ordinal).ToManagedString(false);
        }

        public override int GetOrdinal(string name)
        {
            var columnCount = NativeMethods.Query.DuckDBColumnCount(queryResult);
            for (var i = 0; i < columnCount; i++)
            {
                var columnName = NativeMethods.Query.DuckDBColumnName(queryResult, i).ToManagedString(false);
                if (name == columnName)
                {
                    return i;
                }
            }

            throw new DuckDBException($"Column with name {name} was not found.");
        }

        public override string GetString(int ordinal)
        {
            var unmanagedString = NativeMethods.Types.DuckDBValueVarchar(queryResult, ordinal, currentRow);

            return unmanagedString.ToManagedString();
        }

        public override object GetValue(int ordinal)
        {
            if (IsDBNull(ordinal))
            {
                return DBNull.Value;
            }

            return NativeMethods.Query.DuckDBColumnType(queryResult, ordinal) switch
            {
                DuckDBType.DuckdbTypeInvalid => throw new DuckDBException("Invalid type"),
                DuckDBType.DuckdbTypeBoolean => GetBoolean(ordinal),
                DuckDBType.DuckdbTypeSmallInt => GetInt16(ordinal),
                DuckDBType.DuckdbTypeInteger => GetInt32(ordinal),
                DuckDBType.DuckdbTypeBigInt => GetInt64(ordinal),
                DuckDBType.DuckdbTypeFloat => GetFloat(ordinal),
                DuckDBType.DuckdbTypeDouble => GetDouble(ordinal),
                DuckDBType.DuckdbTypeTimestamp => GetDateTime(ordinal),
                DuckDBType.DuckdbTypeDate => GetDateTime(ordinal),
                DuckDBType.DuckdbTypeTime => GetDateTime(ordinal),
                DuckDBType.DuckdbTypeInterval => throw new NotImplementedException(),
                DuckDBType.DuckdbTypeHugeInt => GetBigInteger(ordinal),
                DuckDBType.DuckdbTypeVarchar => GetString(ordinal),
                DuckDBType.DuckdbTypeDecimal => GetDecimal(ordinal),
                _ => throw new ArgumentException("Unrecognised type")
            };
        }

        public override int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public override Stream GetStream(int ordinal)
        {
            var blob = NativeMethods.Types.DuckDBValueBlob(queryResult, ordinal, currentRow);
            return new DuckDBStream(blob);
        }

        public override bool IsDBNull(int ordinal)
        {
            var nullMask = NativeMethods.Query.DuckDBNullmaskData(queryResult, ordinal);
            return Marshal.ReadByte(nullMask, currentRow) != 0;
        }

        public override int FieldCount { get; }

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => GetValue(GetOrdinal(name));

        public override int RecordsAffected { get; }

        public override bool HasRows { get; }

        public override bool IsClosed => closed;

        public override bool NextResult()
        {
            return false;
        }

        public override bool Read()
        {
            var rowCount = NativeMethods.Query.DuckDBRowCount(queryResult);
            if (currentRow + 1 < rowCount)
            {
                currentRow++;
                return true;
            }

            return false;
        }

        public override int Depth { get; }

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public override void Close()
        {
            if (closed) return;
            
            queryResult.Dispose();

            if (behavior == CommandBehavior.CloseConnection)
            {
                command.CloseConnection();
            }

            closed = true;
        }
    }
}
