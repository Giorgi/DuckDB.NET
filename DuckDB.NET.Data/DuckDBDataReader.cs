using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data
{
    public class DuckDBDataReader : DbDataReader
    {
        private DuckDbCommand command;
        private CommandBehavior behavior;

        private DuckDBResult queryResult;

        private int currentRow = -1;
        private bool closed = false;

        public DuckDBDataReader(DuckDbCommand command, CommandBehavior behavior)
        {
            this.command = command;
            this.behavior = behavior;

            var state = PlatformIndependentBindings.NativeMethods.DuckDBQuery(command.DBNativeConnection, command.CommandText, out queryResult);

            if (state.IsSuccess())
            {
                FieldCount = (int)queryResult.ColumnCount;
            }
            else
            {
                throw new DuckDBException("DuckDBQuery failed", state);
            }
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
            var column = queryResult.Columns[ordinal];
            
            if (column.Type == DuckDBType.DuckdbTypeDate)
            {
                var date = column.ReadAs<DuckDBDate>(currentRow);
                return new DateTime(date.Year, date.Month, date.Day);
            }

            if (column.Type == DuckDBType.DuckdbTypeTimestamp)
            {
                var timestamp = column.ReadAs<DuckDBTimestamp>(currentRow);
                return new DateTime(timestamp.Date.Year, timestamp.Date.Month, timestamp.Date.Day, timestamp.Time.Hour, timestamp.Time.Min, timestamp.Time.Sec, timestamp.Time.Msec);
            }

            throw new InvalidOperationException($"{nameof(GetDateTime)} called on {column.Type} column");
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
            throw new NotImplementedException();
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

        public BigInteger GetBigDecimal(int ordinal)
        {
            return  BigInteger.Parse(PlatformIndependentBindings.NativeMethods.DuckDBValueVarchar(queryResult, ordinal, currentRow));
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
            return PlatformIndependentBindings.NativeMethods.DuckDBValueVarchar(queryResult, ordinal, currentRow);
        }

        public override object GetValue(int ordinal)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
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
            PlatformIndependentBindings.NativeMethods.DuckDBDestroyResult(out queryResult);

            if (behavior == CommandBehavior.CloseConnection)
            {
                command.CloseConnection();
            }

            closed = true;
        }
    }
}