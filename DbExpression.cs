using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Linq2Oracle;

namespace Linq2Oracle.Expressions
{
    public interface IDbExpression
    {
        void SetSqlGenerator(Action<StringBuilder, OracleParameterCollection> sqlGenerator);
        void BuildSql(StringBuilder sql, OracleParameterCollection param);
        object ToDbValue(object value);
    }

    sealed class DbExpression : IDbExpression
    {
        void IDbExpression.SetSqlGenerator(Action<StringBuilder, OracleParameterCollection> sqlGenerator)
        {
            throw new NotImplementedException();
        }

        void IDbExpression.BuildSql(StringBuilder sql, OracleParameterCollection param)
        {
            throw new NotImplementedException();
        }

        object IDbExpression.ToDbValue(object value)
        {
            throw new NotImplementedException();
        }
    }

    public interface IDbExpression<T> : IDbExpression { }

    public sealed class String : IDbExpression<string>
    {
        #region Operators
        public static implicit operator String(string value)
        {
            if (value == null)
                return null;

            return new String().Init((sql, param) => sql.AppendParam(param, value));
        }

        public static Predicate operator ==(String a, String b)
        {
            return a.Equals(b);
        }

        public static Predicate operator !=(String a, String b)
        {
            return a.NotEquals(b);
        }

        public static Predicate operator >(String a, String b)
        {
            return a.GreatThan(b);
        }

        public static Predicate operator >=(String a, String b)
        {
            return a.GreatThanOrEquals(b);
        }

        public static Predicate operator <(String a, String b)
        {
            return a.LessThan(b);
        }

        public static Predicate operator <=(String a, String b)
        {
            return a.LessThanOrEquals(b);
        }

        public static String operator +(String a, String b)
        {
            return new String().Init((sql, param) => sql.Append(a, param).Append(" || ").Append(b, param));
        }
        #endregion

        #region Helper Methods
        String SubString(int startIndex, int? length = null)
        {
            return new String().Init((sql, param) =>
            {
                sql.Append("SUBSTR(").Append(this, param).Append(',').Append(startIndex + 1);
                if (length != null)
                    sql.Append(',').Append(length);
                sql.Append(')');
            });
        }

        String UnaryFunction(string functionName)
        {
            return new String().Init((sql, param) => sql.Append(functionName).Append('(').Append(this, param).Append(')'));
        }
        #endregion

        #region Methods
        public Predicate IsNullOrEmpty()
        {
            return this.IsNull();
        }

        public Predicate IsNullOrWhiteSpace()
        {
            return this.TrimStart().IsNull();
        }

        public Predicate StartsWith(string str)
        {
            return this.Like(str + "%");
        }

        public Predicate EndsWith(string str)
        {
            return this.Like("%" + str);
        }

        public Predicate Contains(string str)
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
                return new Number<int>().Init((sql, param) => sql.Append("LENGTH(").Append(this, param).Append(')'));
            }
        }
        #endregion

        void IDbExpression.SetSqlGenerator(Action<StringBuilder, OracleParameterCollection> sqlGenerator)
        {
            throw new NotImplementedException();
        }

        void IDbExpression.BuildSql(StringBuilder sql, OracleParameterCollection param)
        {
            throw new NotImplementedException();
        }

        object IDbExpression.ToDbValue(object value)
        {
            throw new NotImplementedException();
        }
    }

    public interface INullable<T> : IDbExpression<T> where T : struct { }

    public class Nullable<TExpr, T> : INullable<T>
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

        public static Predicate operator ==(Nullable<TExpr, T> a, Nullable<TExpr, T> b)
        {
            return a.Equals(b);
        }

        public static Predicate operator !=(Nullable<TExpr, T> a, Nullable<TExpr, T> b)
        {
            return a.NotEquals(b);
        }

        public static Predicate operator >(Nullable<TExpr, T> a, Nullable<TExpr, T> b)
        {
            return a.GreatThan(b);
        }

        public static Predicate operator >=(Nullable<TExpr, T> a, Nullable<TExpr, T> b)
        {
            return a.GreatThanOrEquals(b);
        }

        public static Predicate operator <(Nullable<TExpr, T> a, Nullable<TExpr, T> b)
        {
            return a.LessThan(b);
        }

        public static Predicate operator <=(Nullable<TExpr, T> a, Nullable<TExpr, T> b)
        {
            return a.LessThanOrEquals(b);
        }
        #endregion

        #region Methods
        public TExpr GetValueOrDefault(T defaultValue)
        {
            return new TExpr().Init((sql, param) =>
                sql.Append("NVL(").Append(this, param).Append(',').AppendParam(param, defaultValue).Append(')'));
        }
        #endregion

        void IDbExpression.SetSqlGenerator(Action<StringBuilder, OracleParameterCollection> sqlGenerator)
        {
            throw new NotImplementedException();
        }

        void IDbExpression.BuildSql(StringBuilder sql, OracleParameterCollection param)
        {
            throw new NotImplementedException();
        }

        object IDbExpression.ToDbValue(object value)
        {
            throw new NotImplementedException();
        }
    }

    public struct Enum<T> : IDbExpression<T> where T : struct
    {
        void IDbExpression.SetSqlGenerator(Action<StringBuilder, OracleParameterCollection> sqlGenerator)
        {
            throw new NotImplementedException();
        }

        void IDbExpression.BuildSql(StringBuilder sql, OracleParameterCollection param)
        {
            throw new NotImplementedException();
        }


        object IDbExpression.ToDbValue(object value)
        {
            throw new NotImplementedException();
        }
    }

    public struct DateTime : IDbExpression<System.DateTime>
    {
        void IDbExpression.SetSqlGenerator(Action<StringBuilder, OracleParameterCollection> sqlGenerator)
        {
            throw new NotImplementedException();
        }

        void IDbExpression.BuildSql(StringBuilder sql, OracleParameterCollection param)
        {
            throw new NotImplementedException();
        }


        object IDbExpression.ToDbValue(object value)
        {
            throw new NotImplementedException();
        }
    }

    public struct Number<T> : IDbExpression<T> where T : struct
    {
        #region Operators
        public static implicit operator Number<T>(T value)
        {
            return new Number<T>().Init((sql, param) => sql.AppendParam(param, value));
        }

        [Obsolete("This is an unsafe conversion", true)]
        public static implicit operator Number<T>(T? value)
        {
            // C# / VS2012 bug. if Obsolete as error, compiler will choose other version,but code editor tooltip display wrong overloaded method.
            if (!value.HasValue)
                return null;
            return new Number<T>();
        }

        public static explicit operator NullableNumber<T>(Number<T> @this)
        {
            return new NullableNumber<T>();
        }
        
        public static Predicate operator ==(Number<T> a, Number<T> b)
        {
            return new Predicate((sql, param) => sql.Append(a, param).Append(" = ").Append(b, param));
        }

        public static Predicate operator !=(Number<T> a, Number<T> b)
        {
            return new Predicate((sql, param) => sql.Append(a, param).Append(" <> ").Append(b, param));
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
            return new Number<T>().Init((sql, param) =>
                sql.Append('(').Append(a, param).Append(' ').Append(binaryOperator).Append(' ').Append(b, param).Append(')'));
        }
        #endregion

        void IDbExpression.SetSqlGenerator(Action<StringBuilder, OracleParameterCollection> sqlGenerator)
        {
            throw new NotImplementedException();
        }

        void IDbExpression.BuildSql(StringBuilder sql, OracleParameterCollection param)
        {
            throw new NotImplementedException();
        }


        object IDbExpression.ToDbValue(object value)
        {
            throw new NotImplementedException();
        }
    }

    public sealed class NullableEnum<T> : Nullable<Enum<T>, T> where T : struct {
        //public static implicit operator NullableEnum<T>(T? value)
        //{
        //    if (!value.HasValue)
        //        return null;
        //    return new NullableEnum<T>();
        //}
    }

    public sealed class NullableNumber<T> : Nullable<Number<T>, T> where T : struct {
        //public static implicit operator NullableNumber<T>(T? value)
        //{
        //    if (!value.HasValue)
        //        return null;
        //    return new NullableNumber<T>();
        //}
    }

    public sealed class NullableDateTime : Nullable<DateTime, System.DateTime> { }


}
