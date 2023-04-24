using System;
using System.Collections;
using System.Collections.Generic;
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

        private DuckDBResult currentResult;
        private readonly List<DuckDBResult> queryResults;

        private bool closed;
        private long rowCount;
        private int currentRow;
        private int currentResultIndex;

        private int fieldCount;
        private int recordsAffected;

        internal DuckDBDataReader(DuckDbCommand command, List<DuckDBResult> queryResults, CommandBehavior behavior)
        {
            this.command = command;
            this.behavior = behavior;
            this.queryResults = queryResults;

            currentResult = queryResults[0];
            InitReaderData();
        }

        private void InitReaderData()
        {
            currentRow = -1;
            rowCount = NativeMethods.Query.DuckDBRowCount(currentResult);
            fieldCount = (int)NativeMethods.Query.DuckDBColumnCount(currentResult);
            recordsAffected = (int)NativeMethods.Query.DuckDBRowsChanged(currentResult);
        }

        public override bool GetBoolean(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueBoolean(currentResult, ordinal, currentRow);
        }

        public override byte GetByte(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueUInt8(currentResult, ordinal, currentRow);
        }

        private sbyte GetSByte(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueInt8(currentResult, ordinal, currentRow);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override char GetChar(int ordinal)
        {
            throw new NotSupportedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            return NativeMethods.Query.DuckDBColumnType(currentResult, ordinal).ToString();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            var timestampStruct = NativeMethods.Types.DuckDBValueTimestamp(currentResult, ordinal, currentRow);
            
            return timestampStruct.ToDateTime();
        }

        private DuckDBDateOnly GetDateOnly(int ordinal)
        {
            var date = NativeMethods.Types.DuckDBValueDate(currentResult, ordinal, currentRow);
            return NativeMethods.DateTime.DuckDBFromDate(date);
        }

        private DuckDBTimeOnly GetTimeOnly(int ordinal)
        {
            var time = NativeMethods.Types.DuckDBValueTime(currentResult, ordinal, currentRow);
            return NativeMethods.DateTime.DuckDBFromTime(time);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return decimal.Parse(GetString(ordinal), CultureInfo.InvariantCulture);
        }

        public override double GetDouble(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueDouble(currentResult, ordinal, currentRow);
        }

        public override Type GetFieldType(int ordinal)
        {
            return NativeMethods.Query.DuckDBColumnType(currentResult, ordinal) switch
            {
                DuckDBType.DuckdbTypeInvalid => throw new DuckDBException("Invalid type"),
                DuckDBType.DuckdbTypeBoolean => typeof(bool),
                DuckDBType.DuckdbTypeTinyInt => typeof(sbyte),
                DuckDBType.DuckdbTypeSmallInt => typeof(short),
                DuckDBType.DuckdbTypeInteger => typeof(int),
                DuckDBType.DuckdbTypeBigInt => typeof(long),
                DuckDBType.DuckdbTypeUnsignedTinyInt => typeof(byte),
                DuckDBType.DuckdbTypeUnsignedSmallInt => typeof(ushort),
                DuckDBType.DuckdbTypeUnsignedInteger => typeof(uint),
                DuckDBType.DuckdbTypeUnsignedBigInt => typeof(ulong),
                DuckDBType.DuckdbTypeFloat => typeof(float),
                DuckDBType.DuckdbTypeDouble => typeof(double),
                DuckDBType.DuckdbTypeTimestamp => typeof(DateTime),
                DuckDBType.DuckdbTypeInterval => typeof(DuckDBInterval),
                DuckDBType.DuckdbTypeDate => typeof(DuckDBDateOnly),
                DuckDBType.DuckdbTypeTime => typeof(DuckDBTimeOnly),
                DuckDBType.DuckdbTypeHugeInt => typeof(BigInteger),
                DuckDBType.DuckdbTypeVarchar => typeof(string),
                DuckDBType.DuckdbTypeDecimal => typeof(decimal),
                DuckDBType.DuckdbTypeBlob => typeof(Stream),
                var type => throw new ArgumentException($"Unrecognised type {type} ({(int)type}) in column {ordinal + 1}")
            };
        }

        public override float GetFloat(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueFloat(currentResult, ordinal, currentRow);
        }

        public override Guid GetGuid(int ordinal)
        {
            return new Guid(GetString(ordinal));
        }

        public override short GetInt16(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueInt16(currentResult, ordinal, currentRow);
        }

        public override int GetInt32(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueInt32(currentResult, ordinal, currentRow);
        }

        public override long GetInt64(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueInt64(currentResult, ordinal, currentRow);
        }

        private ushort GetUInt16(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueUInt16(currentResult, ordinal, currentRow);
        }

        private uint GetUInt32(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueUInt32(currentResult, ordinal, currentRow);
        }

        private ulong GetUInt64(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueUInt64(currentResult, ordinal, currentRow);
        }

        private BigInteger GetBigInteger(int ordinal)
        {
            return BigInteger.Parse(GetString(ordinal));
        }

        public override string GetName(int ordinal)
        {
            return NativeMethods.Query.DuckDBColumnName(currentResult, ordinal).ToManagedString(false);
        }

        public override int GetOrdinal(string name)
        {
            var columnCount = NativeMethods.Query.DuckDBColumnCount(currentResult);
            for (var i = 0; i < columnCount; i++)
            {
                var columnName = NativeMethods.Query.DuckDBColumnName(currentResult, i).ToManagedString(false);
                if (name == columnName)
                {
                    return i;
                }
            }

            throw new DuckDBException($"Column with name {name} was not found.");
        }

        public override string GetString(int ordinal)
        {
            var unmanagedString = NativeMethods.Types.DuckDBValueVarchar(currentResult, ordinal, currentRow);

            return unmanagedString.ToManagedString();
        }

        public override object GetValue(int ordinal)
        {
            if (IsDBNull(ordinal))
            {
                return DBNull.Value;
            }

            return NativeMethods.Query.DuckDBColumnType(currentResult, ordinal) switch
            {
                DuckDBType.DuckdbTypeInvalid => throw new DuckDBException("Invalid type"),
                DuckDBType.DuckdbTypeBoolean => GetBoolean(ordinal),
                DuckDBType.DuckdbTypeTinyInt => GetSByte(ordinal),
                DuckDBType.DuckdbTypeSmallInt => GetInt16(ordinal),
                DuckDBType.DuckdbTypeInteger => GetInt32(ordinal),
                DuckDBType.DuckdbTypeBigInt => GetInt64(ordinal),
                DuckDBType.DuckdbTypeUnsignedTinyInt => GetByte(ordinal),
                DuckDBType.DuckdbTypeUnsignedSmallInt => GetUInt16(ordinal),
                DuckDBType.DuckdbTypeUnsignedInteger => GetUInt32(ordinal),
                DuckDBType.DuckdbTypeUnsignedBigInt => GetUInt64(ordinal),
                DuckDBType.DuckdbTypeFloat => GetFloat(ordinal),
                DuckDBType.DuckdbTypeDouble => GetDouble(ordinal),
                DuckDBType.DuckdbTypeTimestamp => GetDateTime(ordinal),
                DuckDBType.DuckdbTypeInterval => GetDuckDBInterval(ordinal),
                DuckDBType.DuckdbTypeDate => GetDateOnly(ordinal),
                DuckDBType.DuckdbTypeTime => GetTimeOnly(ordinal),
                DuckDBType.DuckdbTypeHugeInt => GetBigInteger(ordinal),
                DuckDBType.DuckdbTypeVarchar => GetString(ordinal),
                DuckDBType.DuckdbTypeDecimal => GetDecimal(ordinal),
                DuckDBType.DuckdbTypeBlob => GetStream(ordinal),
                var type => throw new ArgumentException($"Unrecognised type {type} ({(int)type}) in column {ordinal + 1}")
            };
        }

        private DuckDBInterval GetDuckDBInterval(int ordinal)
        {
            return NativeMethods.Types.DuckDBValueInterval(currentResult, ordinal, currentRow);
        }

        public override int GetValues(object[] values)
        {
            for (var i = 0; i < FieldCount; i++)
            {
                values[i] = GetValue(i);
            }

            return FieldCount;
        }

        public override Stream GetStream(int ordinal)
        {
            var blob = NativeMethods.Types.DuckDBValueBlob(currentResult, ordinal, currentRow);
            return new DuckDBStream(blob);
        }

        public override bool IsDBNull(int ordinal)
        {
            var nullMask = NativeMethods.Query.DuckDBNullmaskData(currentResult, ordinal);
            return Marshal.ReadByte(nullMask, currentRow) != 0;
        }

        public override int FieldCount => fieldCount;

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => GetValue(GetOrdinal(name));

        public override int RecordsAffected => recordsAffected;

        public override bool HasRows => rowCount > 0;

        public override bool IsClosed => closed;

        public override bool NextResult()
        {
            currentResultIndex++;
            
            if (currentResultIndex < queryResults.Count)
            {
                currentResult = queryResults[currentResultIndex];
                
                InitReaderData();
                return true;
            }

            return false;
        }

        public override bool Read()
        {
            return ++currentRow < rowCount;
        }

        public override int Depth { get; }

        public override IEnumerator GetEnumerator()
        {
            return new DbEnumerator(this, behavior == CommandBehavior.CloseConnection);
        }

        public override DataTable GetSchemaTable()
        {
            DataTable table = new DataTable
            {
                Columns =
                {
                     { "ColumnOrdinal", typeof(int) },
                     { "ColumnName", typeof(string) },
                     { "DataType", typeof(Type) },
                     { "ColumnSize", typeof(int) },
                     { "AllowDBNull", typeof(bool) }
                }
            };
            object[] rowData = new object[5];
            for (int i = 0; i < FieldCount; i++)
            {
                rowData[0] = i;
                rowData[1] = GetName(i);
                rowData[2] = GetFieldType(i);
                rowData[3] = -1;
                rowData[4] = true;
                table.Rows.Add(rowData);
            }
            return table;
        }

        public override void Close()
        {
            if (closed) return;

            foreach (var result in queryResults)
            {
                result.Dispose();
            }

            if (behavior == CommandBehavior.CloseConnection)
            {
                command.CloseConnection();
            }

            closed = true;
        }
    }
}
