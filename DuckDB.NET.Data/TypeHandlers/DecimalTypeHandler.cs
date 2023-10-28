using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Numerics;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class DecimalTypeHandler : BaseTypeHandler, IReadDecimalTypeHandler
    {
        private decimal Power { get; }
        private readonly ITypeHandler InternalTypeHandler;

        public override Type ClrType { get => typeof(decimal); }

        public unsafe DecimalTypeHandler(IntPtr vector, void* dataPointer, ulong* validityMaskPointer)
            : base(vector, dataPointer, validityMaskPointer) 
        {
            var logicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(vector);
            var scale = NativeMethods.LogicalType.DuckDBDecimalScale(logicalType);
            Power = (decimal)Math.Pow(10, scale);

            var decimalType = NativeMethods.LogicalType.DuckDBDecimalInternalType(logicalType);
            InternalTypeHandler = decimalType switch
            {
                DuckDBType.SmallInt => new NumericTypeHandler<short>(vector, dataPointer, validityMaskPointer),
                DuckDBType.Integer => new NumericTypeHandler<int>(vector, dataPointer, validityMaskPointer),
                DuckDBType.BigInt => new NumericTypeHandler<long>(vector, dataPointer, validityMaskPointer),
                DuckDBType.HugeInt => new HugeIntTypeHandler(vector, dataPointer, validityMaskPointer),
                _ => throw new NotSupportedException()
            };
        }

        protected internal decimal GetNative(ulong offset)
            => GetDecimal(offset);

        public decimal GetDecimal(ulong offset)
        {
            return InternalTypeHandler switch
            {
                HugeIntTypeHandler typeHandler => processHugeInt(typeHandler.GetBigInteger(offset), Power),
                NumericTypeHandler<long> typeHandler => processLong(typeHandler.GetValue<long>(offset), (long)Power),
                NumericTypeHandler<int> typeHandler => processInt(typeHandler.GetValue<int>(offset), (int)Power),
                NumericTypeHandler<short> typeHandler => processShort(typeHandler.GetValue<short>(offset), (short)Power),
                _ => throw new NotSupportedException()
            };

            decimal processHugeInt(BigInteger value, decimal power)
            {
                var result = (decimal)BigInteger.DivRem(value, (BigInteger)power, out var remainder);
                result += decimal.Divide((decimal)remainder, power);
                return result;
            }

            decimal processLong(long value, long power)
            {
                var quotient = (decimal)Math.DivRem(value, power, out var remainder);
                var result = quotient + decimal.Divide(remainder, power);
                return result;
            }

            decimal processInt(int value, int power)
            {
                var quotient = (decimal)Math.DivRem(value, power, out var remainder);
                var result = quotient + decimal.Divide(remainder, power);
                return result;
            }

            decimal processShort(short value, short power)
            {
                var quotient = (decimal)Math.DivRem(value, power, out var remainder);
                var result = quotient + decimal.Divide(remainder, power);
                return result;
            }
        }
        public override T GetValue<T>(ulong offset)
            => throw new NotImplementedException();
    }
}
