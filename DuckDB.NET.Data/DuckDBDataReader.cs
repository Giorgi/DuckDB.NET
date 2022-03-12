using DuckDB.NET.Data.Internal;
using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;

namespace DuckDB.NET.Data
{
    public class DuckDBDataReader : DbDataReader
    {
        private readonly DuckDbCommand command;
        private readonly CommandBehavior behavior;

        private DuckDBResult queryResult;

        private int currentRow = -1;
        private bool closed = false;

        public DuckDBDataReader(DuckDbCommand command, CommandBehavior behavior)
        {
            this.command = command;
            this.behavior = behavior;

            using var unmanagedString = command.CommandText.ToUnmanagedString();
            var state = PlatformIndependentBindings.NativeMethods.DuckDBQuery(command.DBNativeConnection, unmanagedString, out queryResult);
            
            if (!string.IsNullOrEmpty(queryResult.ErrorMessage))
            {
                throw new DuckDBException(queryResult.ErrorMessage, state);
            }

            if (!state.IsSuccess())
            {
                throw new DuckDBException("DuckDBQuery failed", state);
            }

            HasRows = queryResult.RowCount > 0;
            FieldCount = (int)queryResult.ColumnCount;
        }

        public override bool GetBoolean(int ordinal)
        {
            return PlatformIndependentBindings.NativeMethods.DuckDBValueBoolean(queryResult, ordinal, currentRow);
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
            return queryResult.Columns[ordinal].Type.ToString();
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
            return PlatformIndependentBindings.NativeMethods.DuckDBValueDouble(queryResult, ordinal, currentRow);
        }

        public override Type GetFieldType(int ordinal)
        {
            return queryResult.Columns[ordinal].Type switch
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
                _ => throw new ArgumentException("Unrecognised type")
            };
        }

        public override float GetFloat(int ordinal)
        {
            return PlatformIndependentBindings.NativeMethods.DuckDBValueFloat(queryResult, ordinal, currentRow);
        }

        public override Guid GetGuid(int ordinal)
        {
            return new Guid(GetString(ordinal));
        }

        public override short GetInt16(int ordinal)
        {
            return PlatformIndependentBindings.NativeMethods.DuckDBValueInt16(queryResult, ordinal, currentRow);
        }

        public override int GetInt32(int ordinal)
        {
            return PlatformIndependentBindings.NativeMethods.DuckDBValueInt32(queryResult, ordinal, currentRow);
        }

        public override long GetInt64(int ordinal)
        {
            return PlatformIndependentBindings.NativeMethods.DuckDBValueInt64(queryResult, ordinal, currentRow);
        }

        public BigInteger GetBigInteger(int ordinal)
        {
            return BigInteger.Parse(GetString(ordinal));
        }

        public override string GetName(int ordinal)
        {
            return queryResult.Columns[ordinal].Name;
        }

        public override int GetOrdinal(string name)
        {
            var index = queryResult.Columns.ToList().FindIndex(c => c.Name == name);

            return index;
        }

        public override string GetString(int ordinal)
        {
            var unmanagedString = PlatformIndependentBindings.NativeMethods.DuckDBValueVarchar(queryResult, ordinal, currentRow);

            return unmanagedString.ToManagedString();
        }

        public override object GetValue(int ordinal)
        {
            return queryResult.Columns[ordinal].Type switch
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
                _ => throw new ArgumentException("Unrecognised type")
            };
        }

        public override int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public override bool IsDBNull(int ordinal)
        {
            return queryResult.Columns[ordinal].NullMask(currentRow);
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
            if (currentRow + 1 < queryResult.RowCount)
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
            PlatformIndependentBindings.NativeMethods.DuckDBDestroyResult(ref queryResult);

            if (behavior == CommandBehavior.CloseConnection)
            {
                command.CloseConnection();
            }

            closed = true;
        }
    }
}