using System;
using System.Collections.Concurrent;
using System.Reflection;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

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

            public bool Equals(DbTypeKey o) => ClrType.Equals(o.ClrType) && DbType == o.DbType;

            public override bool Equals(object obj) 
                => obj is DbTypeKey && Equals((DbTypeKey)obj);
            public override int GetHashCode() => ClrType.GetHashCode() ^ DbType.GetHashCode();
        }

        static readonly ConcurrentDictionary<DbTypeKey, MethodInfo> _GetValueMethodMap = new ConcurrentDictionary<DbTypeKey, MethodInfo>();
        static readonly MethodInfo GetOraString;
        static readonly MethodInfo GetBinary;
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
            GetOraString = type.GetInternalMethod(nameof(_GetOraString));
            GetBinary = type.GetInternalMethod(nameof(_GetBinary));
            GetNullableDate = type.GetInternalMethod(nameof(_GetNullableDate));
            GetNullableTimeStamp = type.GetInternalMethod(nameof(_GetNullableTimeStamp));
            GetNullableEnum_T = type.GetInternalMethod(nameof(_GetNullableEnum));
            GetNullableInt16 = type.GetInternalMethod(nameof(_GetNullableInt16));
            GetNullableInt32 = type.GetInternalMethod(nameof(_GetNullableInt32));
            GetNullableInt64 = type.GetInternalMethod(nameof(_GetNullableInt64));
            GetNullableFloat = type.GetInternalMethod(nameof(_GetNullableFloat));
            GetNullableDouble = type.GetInternalMethod(nameof(_GetNullableDouble));
            GetOraDate = type.GetInternalMethod(nameof(_GetOraDate));
            GetOraTimeStamp = type.GetInternalMethod(nameof(_GetOraTimeStamp));
            GetEnum_T = type.GetInternalMethod(nameof(_GetEnum));
            GetInt16 = type.GetInternalMethod(nameof(_GetInt16));
            GetInt32 = type.GetInternalMethod(nameof(_GetInt32));
            GetInt64 = type.GetInternalMethod(nameof(_GetInt64));
            GetFloat = type.GetInternalMethod(nameof(_GetFloat));
            GetDouble = type.GetInternalMethod(nameof(_GetDouble));
            GetUnknow_T = type.GetInternalMethod(nameof(_UnknowDataType));
            GetOraChar = type.GetInternalMethod(nameof(_GetOraChar));
            GetNullableChar = type.GetInternalMethod(nameof(_GetNullableChar));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="dbType"></param>
        /// <param name="isNullable"></param>
        /// <returns>(OracleDataReader,index) => dbValue</returns>
        internal static MethodInfo GetValueGetMethod(Type clrType, OracleDbType dbType, bool isNullable)
            => _GetValueMethodMap.GetOrAdd(new DbTypeKey(isNullable, clrType, dbType),
                key =>
                {
                    var type = key.ClrType;
                    if (type == typeof(char)) return GetOraChar;
                    if (type == typeof(string)) return GetOraString;
                    if (type == typeof(byte[])) return GetBinary;

                    var columnType = key.DbType;
                    if (key.IsNullable)
                    {
                        if (columnType == OracleDbType.Date) return GetNullableDate;
                        if (columnType == OracleDbType.TimeStamp) return GetNullableTimeStamp;

                        var nullableType = type.GetGenericArguments()[0];
                        if (nullableType.IsEnum) return GetNullableEnum_T.MakeGenericMethod(nullableType);
                        if (nullableType == typeof(char)) return GetNullableChar;
                        if (nullableType == typeof(short)) return GetNullableInt16;
                        if (nullableType == typeof(int)) return GetNullableInt32;
                        if (nullableType == typeof(long)) return GetNullableInt64;
                        if (nullableType == typeof(float)) return GetNullableFloat;
                        if (nullableType == typeof(double)) return GetNullableDouble;
                    }

                    if (columnType == OracleDbType.Date) return GetOraDate;
                    if (columnType == OracleDbType.TimeStamp) return GetOraTimeStamp;
                    if (type.IsEnum) return GetEnum_T.MakeGenericMethod(type);
                    if (type == typeof(short)) return GetInt16;
                    if (type == typeof(int)) return GetInt32;
                    if (type == typeof(long)) return GetInt64;
                    if (type == typeof(float)) return GetFloat;
                    if (type == typeof(double)) return GetDouble;

                    return GetUnknow_T.MakeGenericMethod(type);
                });

        static MethodInfo GetInternalMethod(this Type type, string method) => type.GetMethod(method, BindingFlags.Static | BindingFlags.NonPublic);

        #region OracleDataReader Get Value By Index Helper
        static T _UnknowDataType<T>(this OracleDataReader reader, int index)
        {
            throw new DalException(DbErrorCode.E_DB_UNKNOWN_DATATYPE, "Linq2Oracle unsupported  data type: " + typeof(T).FullName);
        }

        static byte[] _GetBinary(this OracleDataReader reader, int index)
            => reader.GetOracleBinary(index).ToNullable(v => v.Value);

        static short? _GetNullableInt16(this OracleDataReader reader, int index)
            => reader.GetOracleDecimal(index).ToNullable(v => (short)v);

        static short _GetInt16(this OracleDataReader reader, int index) 
            => (short)reader.GetOracleDecimal(index);

        static int? _GetNullableInt32(this OracleDataReader reader, int index) 
            => reader.GetOracleDecimal(index).ToNullable(v => (int)v);

        static int _GetInt32(this OracleDataReader reader, int index) 
            => (int)reader.GetOracleDecimal(index);

        static long? _GetNullableInt64(this OracleDataReader reader, int index) 
            => reader.GetOracleDecimal(index).ToNullable(v => (long)v);

        static long _GetInt64(this OracleDataReader reader, int index) 
            => (long)reader.GetOracleDecimal(index);

        static float? _GetNullableFloat(this OracleDataReader reader, int index)
            => reader.GetOracleDecimal(index).ToNullable(v => (float)v);

        static float _GetFloat(this OracleDataReader reader, int index) 
            => (float)reader.GetOracleDecimal(index);

        static double? _GetNullableDouble(this OracleDataReader reader, int index)
            => reader.GetOracleDecimal(index).ToNullable(v => (double)v);

        static double _GetDouble(this OracleDataReader reader, int index) 
            => (double)reader.GetOracleDecimal(index);

        static System.DateTime _GetOraDate(this OracleDataReader reader, int index) 
            => reader.GetOracleDate(index).Value;

        static System.DateTime? _GetNullableDate(this OracleDataReader reader, int index)
            => reader.GetOracleDate(index).ToNullable(v => v.Value);

        static System.DateTime _GetOraTimeStamp(this OracleDataReader reader, int index) 
            => reader.GetOracleTimeStamp(index).Value;

        static System.DateTime? _GetNullableTimeStamp(this OracleDataReader reader, int index)
            => reader.GetOracleTimeStamp(index).ToNullable(v => v.Value);

        static string _GetOraString(this OracleDataReader reader, int index) 
            => reader.GetOracleString(index).ToNullable(v => v.Value);

        static char _GetOraChar(this OracleDataReader reader, int index) 
            => reader.GetOracleString(index).Value[0];

        static char? _GetNullableChar(this OracleDataReader reader, int index)
        {
            var val = reader.GetOracleString(index);
            return val.IsNull || val.Value.Length == 0 ? null : (char?)val.Value[0];
        }

        static T _GetEnum<T>(this OracleDataReader reader, int index) where T : struct 
            => (T)Enum.Parse(typeof(T), reader.GetOracleString(index).Value);

        static T? _GetNullableEnum<T>(this OracleDataReader reader, int index) where T : struct
            => reader.GetOracleString(index).ToNullable(v => (T)Enum.Parse(typeof(T), v.Value));
        #endregion

        static TValue? ToNullable<T, TValue>(this T nullable, Func<T, TValue> valueGetter)
            where T : INullable
            where TValue : struct 
            => nullable.IsNull ? null : (TValue?)valueGetter(nullable);

        static string ToNullable<T>(this T nullable, Func<T, string> valueGetter)
            where T : INullable
            => nullable.IsNull ? null : valueGetter(nullable);

        static byte[] ToNullable<T>(this T nullable, Func<T, byte[]> valueGetter)
            where T : INullable
            => nullable.IsNull ? null : valueGetter(nullable);
    }
}
