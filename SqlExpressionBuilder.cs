using Oracle.ManagedDataAccess.Client;
using System;

namespace Linq2Oracle.Expressions
{
    using SqlGenerator = Action<SqlContext>;

    static class SqlExpressionBuilder
    {
        public static T Create<T>(this SqlGenerator sqlGenerator) where T : struct, IDbExpression
        {
            return new T { Build = sqlGenerator };
        }

        public static SqlBoolean IsEquals<T1, T2>(this T1 a, T2 b)
            where T1 : IDbExpression
            where T2 : IDbExpression
        {
            if (a.IsNullExpression)
                return b.IsNull();

            if (b.IsNullExpression)
                return a.IsNull();

            return new SqlBoolean(Operatior.Binary(a, "=", b));
        }

        public static SqlBoolean NotEquals<T1, T2>(this T1 a, T2 b)
            where T1 : IDbExpression
            where T2 : IDbExpression
        {
            if (a.IsNullExpression)
                return b.IsNotNull();

            if (b.IsNullExpression)
                return a.IsNotNull();

            return new SqlBoolean(Operatior.Binary(a, "<>", b));
        }

        public static SqlBoolean IsNull<T>(this T a) where T : IDbExpression
        {
            return new SqlBoolean(sql => sql.Append(a).Append(" IS NULL"));
        }

        public static SqlBoolean IsNotNull<T>(this T a) where T : IDbExpression
        {
            return new SqlBoolean(sql => sql.Append(a).Append(" IS NOT NULL"));
        }

        public static SqlBoolean GreatThan<T1, T2>(this T1 a, T2 b)
            where T1 : IDbExpression
            where T2 : IDbExpression
        {
            return new SqlBoolean(Operatior.Binary(a, ">", b));
        }

        public static SqlBoolean GreatThanOrEquals<T1, T2>(this T1 a, T2 b)
            where T1 : IDbExpression
            where T2 : IDbExpression
        {
            return new SqlBoolean(Operatior.Binary(a, ">=", b));
        }

        public static SqlBoolean LessThan<T1, T2>(this T1 a, T2 b)
            where T1 : IDbExpression
            where T2 : IDbExpression
        {
            return new SqlBoolean(Operatior.Binary(a, "<", b));
        }

        public static SqlBoolean LessThanOrEquals<T1, T2>(this T1 a, T2 b)
            where T1 : IDbExpression
            where T2 : IDbExpression
        {
            return new SqlBoolean(Operatior.Binary(a, "<=", b));
        }
    }

    static class Operatior
    {
        public static SqlGenerator Binary<T1,T2>(T1 a, string binaryOperator, T2 b)
            where T1 : IDbExpression
            where T2 : IDbExpression
        {
            return sql => sql.Append('(').Append(a).Append(' ').Append(binaryOperator).Append(' ').Append(b).Append(')');
        }

        public static SqlGenerator Unary<T>(string unaryOperator, T a)
            where T : IDbExpression
        {
            return sql => sql.Append(unaryOperator).Append('(').Append(a).Append(')');
        }
    }

    static class Function
    {
        public static SqlGenerator Count()
        {
            return sql => sql.Append("COUNT(*)");
        }

        public static SqlGenerator Call<T>(string function, T param1)
            where T : IDbExpression
        {
            return sql => sql.Append(function).Append('(').Append(param1).Append(')');
        }

        public static SqlGenerator Call<T1, T2>(string function, T1 param1, T2 param2)
            where T1 : IDbExpression
            where T2 : IDbExpression
        {
            return sql => sql.Append(function).Append('(').Append(param1).Append(',').Append(param2).Append(')');
        }

        //public static SqlGenerator Call(string function, SqlGenerator param1, int param2)
        //{
        //    return sql => sql.Append(function).Append('(').Append(param1).Append(',').Append(param2).Append(')');
        //}

        public static SqlGenerator Call<T1, T2, T3>(string function, T1 param1, T2 param2, T3 param3)
            where T1 : IDbExpression
            where T2 : IDbExpression
            where T3 : IDbExpression
        {
            return sql => sql.Append(function).Append('(').Append(param1).Append(',').Append(param2).Append(',').Append(param3).Append(')');
        }
    }

    static class SqlParameter
    {
        public static SqlGenerator Create<T>(T value)
        {
            return sql => sql.AppendParam(value);
        }

        public static SqlGenerator Create<T>(T value, OracleDbType dbType)
        {
            return sql => sql.AppendParam(dbType, value);
        }
    }
}
