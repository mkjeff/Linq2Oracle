using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Linq2Oracle
{
    static class OracleDataReaderHelper
    {
        struct DbTypeKey
        {
            public readonly Type ClrType;
            public readonly OracleDbType DbType;
            public readonly bool IsNullable;
            public DbTypeKey(bool nullable, Type clrType, OracleDbType dbType)
            {
                IsNullable = nullable;
                ClrType = clrType;
                DbType = dbType;
            }

            public bool Equals(DbTypeKey o)
            {
                return ClrType.Equals(o.ClrType) && DbType == o.DbType;
            }

            public override bool Equals(object obj)
            {
                if (obj is DbTypeKey)
                    return Equals((DbTypeKey)obj);
                return false;
            }
            public override int GetHashCode()
            {
                return ClrType.GetHashCode() ^ DbType.GetHashCode();
            }
        }

        static readonly ConcurrentDictionary<DbTypeKey, MethodInfo> _GetValueMethodMap = new ConcurrentDictionary<DbTypeKey, MethodInfo>();
        static readonly MethodInfo GetOraString;
        static readonly MethodInfo GetBinary;
        static readonly MethodInfo GetBlob;
        static readonly MethodInfo GetNullableDate;
        static readonly MethodInfo GetNullableTimeStamp;
        static readonly MethodInfo GetNullableEnum_T;
        static readonly MethodInfo GetNullableInt16;
        static readonly MethodInfo GetNullableInt32;
        static readonly MethodInfo GetNullableInt64;
        static readonly MethodInfo GetNullableFloat;
        static readonly MethodInfo GetNullableDouble;
        static readonly MethodInfo GetOraDate;
        static readonly MethodInfo GetOraTimeStamp;
        static readonly MethodInfo GetEnum_T;
        static readonly MethodInfo GetInt16;
        static readonly MethodInfo GetInt32;
        static readonly MethodInfo GetInt64;
        static readonly MethodInfo GetFloat;
        static readonly MethodInfo GetDouble;
        static readonly MethodInfo GetUnknow_T;
        static readonly MethodInfo GetOraChar;
        static readonly MethodInfo GetNullableChar;

        static OracleDataReaderHelper()
        {
            var type = typeof(OracleDataReaderHelper);
            GetOraString = type.GetInternalMethod("_GetOraString");
            GetBinary = type.GetInternalMethod("_GetBinary");
            GetBlob = type.GetInternalMethod("_GetBlob");
            GetNullableDate = type.GetInternalMethod("_GetNullableDate");
            GetNullableTimeStamp = type.GetInternalMethod("_GetNullableTimeStamp");
            GetNullableEnum_T = type.GetInternalMethod("_GetNullableEnum");
            GetNullableInt16 = type.GetInternalMethod("_GetNullableInt16");
            GetNullableInt32 = type.GetInternalMethod("_GetNullableInt32");
            GetNullableInt64 = type.GetInternalMethod("_GetNullableInt64");
            GetNullableFloat = type.GetInternalMethod("_GetNullableFloat");
            GetNullableDouble = type.GetInternalMethod("_GetNullableDouble");
            GetOraDate = type.GetInternalMethod("_GetOraDate");
            GetOraTimeStamp = type.GetInternalMethod("_GetOraTimeStamp");
            GetEnum_T = type.GetInternalMethod("_GetEnum");
            GetInt16 = type.GetInternalMethod("_GetInt16");
            GetInt32 = type.GetInternalMethod("_GetInt32");
            GetInt64 = type.GetInternalMethod("_GetInt64");
            GetFloat = type.GetInternalMethod("_GetFloat");
            GetDouble = type.GetInternalMethod("_GetDouble");
            GetUnknow_T = type.GetInternalMethod("_UnknowDataType");
            GetOraChar = type.GetInternalMethod("_GetOraChar");
            GetNullableChar = type.GetInternalMethod("_GetNullableChar");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="dbType"></param>
        /// <param name="isNullable"></param>
        /// <returns>(OracleDataReader,index) => dbValue</returns>
        internal static MethodInfo GetValueGetMethod(Type clrType, OracleDbType dbType, bool isNullable)
        {
            return _GetValueMethodMap.GetOrAdd(new DbTypeKey(isNullable, clrType, dbType), key =>
            {
                if (clrType == typeof(char)) return GetOraChar;
                if (clrType == typeof(string)) return GetOraString;
                if (clrType == typeof(byte[]))
                    if (dbType == OracleDbType.Raw)
                        return GetBinary;
                    else if (dbType == OracleDbType.Blob)
                        return GetBlob;

                if (isNullable)
                {
                    if (dbType == OracleDbType.Date) return GetNullableDate;
                    if (dbType == OracleDbType.TimeStamp) return GetNullableTimeStamp;

                    var nullableType = clrType.GetGenericArguments()[0];
                    if (nullableType.IsEnum) return GetNullableEnum_T.MakeGenericMethod(nullableType);
                    if (nullableType == typeof(char)) return GetNullableChar;
                    if (nullableType == typeof(short)) return GetNullableInt16;
                    if (nullableType == typeof(int)) return GetNullableInt32;
                    if (nullableType == typeof(long)) return GetNullableInt64;
                    if (nullableType == typeof(float)) return GetNullableFloat;
                    if (nullableType == typeof(double)) return GetNullableDouble;
                }

                if (dbType == OracleDbType.Date) return GetOraDate;
                if (dbType == OracleDbType.TimeStamp) return GetOraTimeStamp;
                if (clrType.IsEnum) return GetEnum_T.MakeGenericMethod(clrType);
                if (clrType == typeof(short)) return GetInt16;
                if (clrType == typeof(int)) return GetInt32;
                if (clrType == typeof(long)) return GetInt64;
                if (clrType == typeof(float)) return GetFloat;
                if (clrType == typeof(double)) return GetDouble;

                return GetUnknow_T.MakeGenericMethod(clrType);
            });
        }

        static MethodInfo GetInternalMethod(this Type type, string method)
        {
            return type.GetMethod(method, BindingFlags.Static | BindingFlags.NonPublic);
        }
        #region OracleDataReader Get Value By Index Helper
        static T _UnknowDataType<T>(this OracleDataReader reader, int index)
        {
            throw new DalException(DbErrorCode.E_DB_UNKNOWN_DATATYPE, "Linq2Oracle unsupported  data type: " + typeof(T).FullName);
        }

        static byte[] _GetBinary(this OracleDataReader reader, int index)
        {
            var val = reader.GetOracleBinary(index);
            return val.IsNull ? null : val.Value;
        }

        static byte[] _GetBlob(this OracleDataReader reader, int index)
        {
            var val = reader.GetOracleBlob(index);
            return val.IsNull ? null : val.Value;
        }

        static short? _GetNullableInt16(this OracleDataReader reader, int index)
        {
            var val = reader.GetOracleDecimal(index);
            return val.IsNull ? (short?)null : (short)val;
        }

        static short _GetInt16(this OracleDataReader reader, int index)
        {
            return (short)reader.GetOracleDecimal(index);
        }

        static int? _GetNullableInt32(this OracleDataReader reader, int index)
        {
            var val = reader.GetOracleDecimal(index);
            return val.IsNull ? (int?)null : (int)val;
        }

        static int _GetInt32(this OracleDataReader reader, int index)
        {
            return (int)reader.GetOracleDecimal(index);
        }

        static long? _GetNullableInt64(this OracleDataReader reader, int index)
        {
            var val = reader.GetOracleDecimal(index);
            return val.IsNull ? (long?)null : (long)val;
        }

        static long _GetInt64(this OracleDataReader reader, int index)
        {
            return (long)reader.GetOracleDecimal(index);
        }

        static float? _GetNullableFloat(this OracleDataReader reader, int index)
        {
            var val = reader.GetOracleDecimal(index);
            return val.IsNull ? (float?)null : (float)val;
        }

        static float _GetFloat(this OracleDataReader reader, int index)
        {
            return (float)reader.GetOracleDecimal(index);
        }

        static double? _GetNullableDouble(this OracleDataReader reader, int index)
        {
            var val = reader.GetOracleDecimal(index);
            return val.IsNull ? (double?)null : (double)val;
        }

        static double _GetDouble(this OracleDataReader reader, int index)
        {
            return (double)reader.GetOracleDecimal(index);
        }

        static DateTime _GetOraDate(this OracleDataReader reader, int index)
        {
            return reader.GetOracleDate(index).Value;
        }

        static DateTime? _GetNullableDate(this OracleDataReader reader, int index)
        {
            var val = reader.GetOracleDate(index);
            return val.IsNull ? null : (DateTime?)val.Value;
        }

        static DateTime _GetOraTimeStamp(this OracleDataReader reader, int index)
        {
            return reader.GetOracleTimeStamp(index).Value;
        }

        static DateTime? _GetNullableTimeStamp(this OracleDataReader reader, int index)
        {
            var val = reader.GetOracleTimeStamp(index);
            return val.IsNull ? null : (DateTime?)val.Value;
        }

        static string _GetOraString(this OracleDataReader reader, int index)
        {
            var val = reader.GetOracleString(index);
            return val.IsNull ? null : val.Value;
        }

        static char _GetOraChar(this OracleDataReader reader, int index)
        {
            return reader.GetOracleString(index).Value[0];
        }

        static char? _GetNullableChar(this OracleDataReader reader, int index)
        {
            var val = reader.GetOracleString(index);
            return val.IsNull || val.Value.Length == 0 ? null : (char?)val.Value[0];
        }

        static T _GetEnum<T>(this OracleDataReader reader, int index) where T : struct
        {
            return (T)Enum.Parse(typeof(T), reader.GetOracleString(index).Value);
        }

        static T? _GetNullableEnum<T>(this OracleDataReader reader, int index) where T : struct
        {
            var val = reader.GetOracleString(index);
            return val.IsNull ? null : (T?)Enum.Parse(typeof(T), val.Value);
        }
        #endregion
    }
}
