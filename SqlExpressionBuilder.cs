using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Linq2Oracle.Expressions
{
    interface ISqlExpressionBuilder
    {
        Action<SqlContext> Build { get; set; }
        OracleDbType DbType { get; set; }
    }

    static class SqlExpressionBuilder
    {
        public static T Init<T>(this T column, OracleDbType dbType, Action<SqlContext> sqlGenerator) where T : ISqlExpressionBuilder
        {
            column.Build = sqlGenerator;
            return column;
        }

        public static Boolean IsEquals(this IDbExpression a, IDbExpression b)
        {          
            if (a == null)
                return b.IsNull();

            if (b == null)
                return a.IsNull();

            return new Boolean(sql => sql.Append(a).Append(" = ").Append(b));
        }

        public static Boolean NotEquals(this IDbExpression a, IDbExpression b)
        {
            if (a == null)
                return b.IsNotNull();

            if (b == null)
                return a.IsNotNull();

            return new Boolean(sql => sql.Append(a).Append(" <> ").Append(b));
        }

        public static Boolean IsNull(this IDbExpression a)
        {
            return new Boolean(sql => sql.Append(a).Append(" IS NULL"));
        }

        public static Boolean IsNotNull(this IDbExpression a)
        {
            return new Boolean(sql => sql.Append(a).Append(" IS NOT NULL"));
        }

        public static Boolean GreatThan(this IDbExpression a, IDbExpression b)
        {
            if (a == null && b == null)
                throw new ArgumentNullException("a and b", "can't apply comparison operator with NULL");
            return new Boolean(sql => sql.Append(a).Append(" > ").Append(b));
        }

        public static Boolean GreatThanOrEquals(this IDbExpression a, IDbExpression b)
        {
            if (a == null && b == null)
                throw new ArgumentNullException("a and b", "can't apply comparison operator with NULL");
            return new Boolean(sql => sql.Append(a).Append(" >= ").Append(b));
        }

        public static Boolean LessThan(this IDbExpression a, IDbExpression b)
        {
            if (a == null && b == null)
                throw new ArgumentNullException("a and b", "can't apply comparison operator with NULL");
            return new Boolean(sql => sql.Append(a).Append(" < ").Append(b));
        }

        public static Boolean LessThanOrEquals(this IDbExpression a, IDbExpression b)
        {
            if (a == null && b == null)
                throw new ArgumentNullException("a and b", "can't apply comparison operator with NULL");
            return new Boolean(sql => sql.Append(a).Append(" <= ").Append(b));
        }

        public static Boolean Like(this String a, string pattern)
        {
            return new Boolean(sql => sql.Append(a).Append(" LIKE ").AppendParam(pattern));
        }

         
    }
}
