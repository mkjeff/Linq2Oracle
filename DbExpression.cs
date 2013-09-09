using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Linq2Oracle;

namespace Linq2Oracle.Expressions
{
    using SqlGenerator = Action<SqlContext>;

    public interface IDbExpression
    {
        void Build(SqlContext sql);
        OracleDbType DbType { get; }
    }

    public interface IDbExpression<T> : IDbExpression { }

    sealed class DbExpression : IDbExpression {
        readonly SqlGenerator _generator;
        internal DbExpression(SqlGenerator generator)
        {
            _generator = generator;
        }
        public void Build(SqlContext sql)
        {
            _generator(sql);
        }

        public OracleDbType DbType
        {
            get { throw new NotImplementedException(); }
        }
    }

    public sealed class String : IDbExpression<string>, ISqlExpressionBuilder
    {
        #region Operators
        public static implicit operator String(string value)
        {
            if (value == null)
                return null;

            return new String().Init(OracleDbType.Varchar2, sql => sql.AppendParam(value));
        }

        public static Boolean operator ==(String a, String b)
        {
            return a.IsEquals(b);
        }

        public static Boolean operator !=(String a, String b)
        {
            return a.NotEquals(b);
        }

        public static Boolean operator >(String a, String b)
        {
            return a.GreatThan(b);
        }

        public static Boolean operator >=(String a, String b)
        {
            return a.GreatThanOrEquals(b);
        }

        public static Boolean operator <(String a, String b)
        {
            return a.LessThan(b);
        }

        public static Boolean operator <=(String a, String b)
        {
            return a.LessThanOrEquals(b);
        }

        public static String operator +(String a, String b)
        {
            return new String().Init(OracleDbType.Varchar2, sql => sql.Append(a).Append(" || ").Append(b));
        }
        #endregion

        #region Helper Methods
        String SubString(int startIndex, int? length = null)
        {
            return new String().Init(OracleDbType.Varchar2, sql =>
            {
                sql.Append("SUBSTR(").Append(this).Append(',').Append(startIndex + 1);
                if (length != null)
                    sql.Append(',').Append(length.Value);
                sql.Append(')');
            });
        }

        String UnaryFunction(string functionName)
        {
            return new String().Init(OracleDbType.Varchar2, sql => sql.Append(functionName).Append('(').Append(this).Append(')'));
        }
        #endregion

        #region Methods
        public Boolean IsNullOrEmpty()
        {
            return this.IsNull();
        }

        public Boolean IsNullOrWhiteSpace()
        {
            return this.TrimStart().IsNull();
        }

        public Boolean StartsWith(string str)
        {
            return this.Like(str + "%");
        }

        public Boolean EndsWith(string str)
        {
            return this.Like("%" + str);
        }

        public Boolean Contains(string str)
        {
            return this.Like("%" + str + "%");
        }

        // TO DO: SQL version compare
        //public Predicate StartsWith(String str)
        //{
        //    return this.Like(str + "%");
        //}

        //public Predicate EndsWith(String str)
        //{
        //    return this.Like("%" + str);
        //}

        //public Predicate Contains(String str)
        //{
        //    return this.Like("%" + str + "%");
        //}

        public String Substring(int startIndex, int length)
        {
            return Substring(startIndex, length);
        }

        public String Substring(int startIndex)
        {
            return Substring(startIndex);
        }

        public String Trim()
        {
            return UnaryFunction("TRIM");
        }

        public String TrimStart()
        {
            return UnaryFunction("LTRIM");
        }

        public String TrimEnd()
        {
            return UnaryFunction("RTRIM");
        }

        public String ToLower()
        {
            return UnaryFunction("LOWER");
        }

        public String ToUpper()
        {
            return UnaryFunction("UPPER");
        }
        #endregion

        #region Properties
        public Number<int> Length
        {
            get
            {
                return new Number<int>().Init(OracleDbType.Int32, sql => sql.Append("LENGTH(").Append(this).Append(')'));
            }
        }
        #endregion

        SqlGenerator ISqlExpressionBuilder.Build { get; set; }

        void IDbExpression.Build(SqlContext sql)
        {
            ((ISqlExpressionBuilder)this).Build(sql);
        }

        public OracleDbType DbType { get; set; }
    }

    public interface INullable<T> : IDbExpression<T> where T : struct { }

    public class Nullable<TExpr, T> : INullable<T>, ISqlExpressionBuilder
        where TExpr : struct,IDbExpression<T>
        where T : struct
    {
        #region Operators
        public static implicit operator Nullable<TExpr, T>(T? value)
        {
            if (!value.HasValue)
                return null;
            return new Nullable<TExpr, T>();
        }

        public static Boolean operator ==(Nullable<TExpr, T> a, Nullable<TExpr, T> b)
        {
            return a.IsEquals(b);
        }

        public static Boolean operator !=(Nullable<TExpr, T> a, Nullable<TExpr, T> b)
        {
            return a.NotEquals(b);
        }

        public static Boolean operator >(Nullable<TExpr, T> a, Nullable<TExpr, T> b)
        {
            return a.GreatThan(b);
        }

        public static Boolean operator >=(Nullable<TExpr, T> a, Nullable<TExpr, T> b)
        {
            return a.GreatThanOrEquals(b);
        }

        public static Boolean operator <(Nullable<TExpr, T> a, Nullable<TExpr, T> b)
        {
            return a.LessThan(b);
        }

        public static Boolean operator <=(Nullable<TExpr, T> a, Nullable<TExpr, T> b)
        {
            return a.LessThanOrEquals(b);
        }
        #endregion

        #region Methods
        public TExpr GetValueOrDefault(T defaultValue)
        {
            var result = new TExpr();
            ((ISqlExpressionBuilder)result).Init(result.DbType, sql =>
                sql.Append("NVL(").Append(this).Append(',').AppendParam(defaultValue).Append(')'));
            return result;
        }
        #endregion

        SqlGenerator ISqlExpressionBuilder.Build { get; set; }

        void IDbExpression.Build(SqlContext sql)
        {
            ((ISqlExpressionBuilder)this).Build(sql);
        }

        public OracleDbType DbType { get; set; }
    }

    public struct Enum<T> : IDbExpression<T>, ISqlExpressionBuilder where T : struct
    {
        SqlGenerator ISqlExpressionBuilder.Build { get; set; }

        void IDbExpression.Build(SqlContext tableAlias)
        {
            ((ISqlExpressionBuilder)this).Build(tableAlias);
        }

        public OracleDbType DbType { get; set; }
    }

    public struct DateTime : IDbExpression<System.DateTime>, ISqlExpressionBuilder
    {
        SqlGenerator ISqlExpressionBuilder.Build { get; set; }

        void IDbExpression.Build(SqlContext tableAlias)
        {
            ((ISqlExpressionBuilder)this).Build(tableAlias);
        }

        public OracleDbType DbType { get; set; }
    }

    public struct Number<T> : IDbExpression<T>, ISqlExpressionBuilder where T : struct
    {
        #region Operators
        public static implicit operator Number<T>(T value)
        {
            return new Number<T>().Init(OracleDbType.Decimal, sql => sql.AppendParam(value));
        }

        [Obsolete("This is an unsafe conversion", false)]
        public static implicit operator Number<T>(T? value)
        {
            // C# / Visual Studio bug. if Obsolete as error, compiler will choose other version,but code editor tooltip display wrong overloaded method.
            if (!value.HasValue)
                return null;
            return new Number<T>();
        }

        public static explicit operator NullableNumber<T>(Number<T> @this)
        {
            return new NullableNumber<T>();
        }

        public static Boolean operator ==(Number<T> a, Number<T> b)
        {
            return a.IsEquals(b);
        }

        public static Boolean operator !=(Number<T> a, Number<T> b)
        {
            return a.NotEquals(b);
        }

        public static Number<T> operator +(Number<T> a, Number<T> b)
        {
            return BuildBinaryExpression(a, b, '+');
        }

        public static Number<T> operator -(Number<T> a, Number<T> b)
        {
            return BuildBinaryExpression(a, b, '-');
        }

        public static Number<T> operator *(Number<T> a, Number<T> b)
        {
            return BuildBinaryExpression(a, b, '*');
        }

        public static Number<T> operator /(Number<T> a, Number<T> b)
        {
            return BuildBinaryExpression(a, b, '/');
        }

        private static Number<T> BuildBinaryExpression(Number<T> a, Number<T> b, char binaryOperator)
        {
            return new Number<T>().Init(a.DbType,
                sql => sql.Append('(').Append(a).Append(' ').Append(binaryOperator).Append(' ').Append(b).Append(')'));
        }
        #endregion

        SqlGenerator ISqlExpressionBuilder.Build { get; set; }

        void IDbExpression.Build(SqlContext sql)
        {
            ((ISqlExpressionBuilder)this).Build(sql);
        }

        public OracleDbType DbType { get; set; }
    }

    public sealed class NullableEnum<T> : Nullable<Enum<T>, T> where T : struct
    {
        //public static implicit operator NullableEnum<T>(T? value)
        //{
        //    if (!value.HasValue)
        //        return null;
        //    return new NullableEnum<T>();
        //}
    }

    public sealed class NullableNumber<T> : Nullable<Number<T>, T> where T : struct
    {
        //public static implicit operator NullableNumber<T>(T? value)
        //{
        //    if (!value.HasValue)
        //        return null;
        //    return new NullableNumber<T>();
        //}
    }

    public sealed class NullableDateTime : Nullable<DateTime, System.DateTime> { }


}
