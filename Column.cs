using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Linq2Oracle
{
    interface IDbExpression
    {
        void SetColumnInfo(string expr, IDbMetaInfo column);
    }

    public abstract class DbExpression : IDbExpression
    {
        internal IDbMetaInfo MetaInfo { get; private set; }
        internal OracleDbType DbType { get { return MetaInfo.DbType; } }
        internal int Size { get { return MetaInfo.Size; } }
        internal string Expression { get; private set; }

        void IDbExpression.SetColumnInfo(string expr, IDbMetaInfo column)
        {
            this.MetaInfo = column;
            this.Expression = expr;
        }
    }

    public class DbExpression<T> : DbExpression
    {
        public virtual Predicate Equals(T value)
        {
            return value == null
                ? new Predicate((sql, param) => sql.Append(Expression).Append(" IS NULL"))
                : new Predicate((sql, param) => sql.Append(Expression).Append(" = ").AppendParam(param, DbType, Size, value));
        }
        public virtual Predicate NotEquals(T value)
        {
            return value == null
                ? new Predicate((sql, param) => sql.Append(Expression).Append(" IS NOT NULL"))
                : new Predicate((sql, param) => sql.Append(Expression).Append(" <> ").AppendParam(param, DbType, Size, value));
        }
        public static Predicate operator ==(DbExpression<T> columna, DbExpression<T> columnb)
        {
            return new Predicate((sql, param) => sql.Append(columna.Expression).Append(" = ").Append(columnb.Expression));
        }
        public static Predicate operator !=(DbExpression<T> columna, DbExpression<T> columnb)
        {
            return new Predicate((sql, param) => sql.Append(columna.Expression).Append(" <> ").Append(columnb.Expression));
        }
        public static Predicate operator ==(DbExpression<T> column, T value)
        {
            return column.Equals(value);
        }
        public static Predicate operator ==(T value, DbExpression<T> column)
        {
            return column.Equals(value);
        }
        public static Predicate operator !=(DbExpression<T> column, T value)
        {
            return column.NotEquals(value);
        }
        public static Predicate operator !=(T value, DbExpression<T> column)
        {
            return column.NotEquals(value);
        }

        public static Predicate operator >(T value, DbExpression<T> column) { return column < value; }
        public static Predicate operator >(DbExpression<T> column, T value)
        {
            if (value == null) throw new DalException(DbErrorCode.E_DB_SQL_INVAILD, "Cannot apply comparison operator with a null value argument.");
            return new Predicate((sql, param) => sql.Append(column.Expression).Append(" > ").AppendParam(param, column.DbType, column.Size, value));
        }
        public static Predicate operator >=(T value, DbExpression<T> column) { return column <= value; }
        public static Predicate operator >=(DbExpression<T> column, T value)
        {
            if (value == null) throw new DalException(DbErrorCode.E_DB_SQL_INVAILD, "Cannot apply comparison operator with a null value argument.");
            return new Predicate((sql, param) => sql.Append(column.Expression).Append(" >= ").AppendParam(param, column.DbType, column.Size, value));
        }
        public static Predicate operator <(T value, DbExpression<T> column) { return column > value; }
        public static Predicate operator <(DbExpression<T> column, T value)
        {
            if (value == null) throw new DalException(DbErrorCode.E_DB_SQL_INVAILD, "Cannot apply comparison operator with a null value argument.");
            return new Predicate((sql, param) => sql.Append(column.Expression).Append(" < ").AppendParam(param, column.DbType, column.Size, value));
        }
        public static Predicate operator <=(T value, DbExpression<T> column) { return column >= value; }
        public static Predicate operator <=(DbExpression<T> column, T value)
        {
            if (value == null) throw new DalException(DbErrorCode.E_DB_SQL_INVAILD, "Cannot apply comparison operator with a null value argument.");
            return new Predicate((sql, param) => sql.Append(column.Expression).Append(" <= ").AppendParam(param, column.DbType, column.Size, value));
        }

        public Predicate IsNull { get { return new Predicate((sql, param) => sql.Append(Expression).Append(" IS NULL")); } }
        public Predicate IsNotNull { get { return new Predicate((sql, param) => sql.Append(Expression).Append(" IS NOT NULL")); } }
    }

    public sealed class StringColumn : DbExpression<string>
    {
        public override Predicate Equals(string value)
        {
            return string.IsNullOrEmpty(value)
                ? new Predicate((sql, param) => sql.Append(Expression).Append(" IS NULL"))
                : new Predicate((sql, param) => sql.Append(Expression).Append(" = ").AppendParam(param, DbType, Size, value));
        }
        public override Predicate NotEquals(string value)
        {
            return string.IsNullOrEmpty(value)
                ? new Predicate((sql, param) => sql.Append(Expression).Append(" IS NOT NULL"))
                : new Predicate((sql, param) => sql.Append(Expression).Append(" <> ").AppendParam(param, DbType, Size, value));
        }
        public Predicate StartsWith(string str)
        {
            if (string.IsNullOrEmpty(str)) throw new DalException(DbErrorCode.E_DB_SQL_INVAILD, "String Column StartWith argument can not be null or empty");
            return new Predicate((sql, param) => sql.Append(Expression).Append(" LIKE ").AppendParam(param, DbType, Size, str + "%"));
        }

        public Predicate EndsWith(string str)
        {
            if (string.IsNullOrEmpty(str)) throw new DalException(DbErrorCode.E_DB_SQL_INVAILD, "String Column EndWith argument can not be null or empty");
            return new Predicate((sql, param) => sql.Append(Expression).Append(" LIKE ").AppendParam(param, DbType, Size, "%" + str));
        }

        public Predicate Contains(string str)
        {
            if (string.IsNullOrEmpty(str)) throw new DalException(DbErrorCode.E_DB_SQL_INVAILD, "String Column Contains argument can not be null or empty");
            return new Predicate((sql, param) => sql.Append(Expression).Append(" LIKE ").AppendParam(param, DbType, Size, "%" + str + "%"));
        }

        public StringColumn Substring(int startIndex, int length)
        {
            return new StringColumn().Init("SUBSTR(" + Expression + "," + (startIndex + 1) + "," + length + ")", MetaInfo);
        }

        public StringColumn Substring(int startIndex)
        {
            return new StringColumn().Init("SUBSTR(" + Expression + "," + (startIndex + 1) + ")", MetaInfo);
        }

        public StringColumn Trim()
        {
            return new StringColumn().Init("TRIM(" + Expression + ")", MetaInfo);
        }

        public StringColumn TrimStart()
        {
            return new StringColumn().Init("LTRIM(" + Expression + ")", MetaInfo);
        }

        public StringColumn TrimEnd()
        {
            return new StringColumn().Init("RTRIM(" + Expression + ")", MetaInfo);
        }

        public NumberColumn<int> Length
        {
            get
            {
                return new NumberColumn<int>().Init("LENGTH(" + Expression + ")", new DbExpressionMetaInfo
                {
                    DbType = OracleDbType.Int32,
                    Size = 4
                });
            }
        }
    }

    public sealed class EnumColumn<T> : DbExpression<T>
    {
    }

    public abstract class RangeColumn<T> : DbExpression<T>
    {
        public Predicate Between(T start, T end)
        {
            if (start == null || end == null) throw new DalException(DbErrorCode.E_DB_SQL_INVAILD, "SQL Between condition can not be NULL");
            return new Predicate((sql, param) => sql.Append(Expression)
                .Append(" BETWEEN ").AppendParam(param, DbType, Size, start).Append(" AND ").AppendParam(param, DbType, Size, end));
        }
    }

    public sealed class DateTimeColumn<T> : RangeColumn<T>
    {
    }

    public sealed class NumberColumn<T> : RangeColumn<T>
    {
        public static NumberColumn<T> operator +(NumberColumn<T> a, NumberColumn<T> b)
        {
            return BuildBinaryExpression(a, b, "+", a.MetaInfo);
        }

        public static NumberColumn<T> operator -(NumberColumn<T> a, NumberColumn<T> b)
        {
            return BuildBinaryExpression(a, b, "-", a.MetaInfo);
        }

        public static NumberColumn<T> operator *(NumberColumn<T> a, NumberColumn<T> b)
        {
            return BuildBinaryExpression(a, b, "*", a.MetaInfo);
        }

        public static NumberColumn<T> operator /(NumberColumn<T> a, NumberColumn<T> b)
        {
            return BuildBinaryExpression(a, b, "/", a.MetaInfo);
        }

        private static NumberColumn<T> BuildBinaryExpression(NumberColumn<T> a, NumberColumn<T> b, string binaryOperator,IDbMetaInfo metaInfo)
        {
            return new NumberColumn<T>().Init("(" + a.Expression + " " + binaryOperator + " " + b.Expression + ")", metaInfo);
        }
    }
}
