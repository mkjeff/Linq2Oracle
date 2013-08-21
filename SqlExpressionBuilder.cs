using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Linq2Oracle.Expressions
{
    static class SqlExpressionBuilder
    {
        public static Predicate Equals(this IDbExpression a, IDbExpression b)
        {          
            if (a == null)
                return b.IsNull();

            if (b == null)
                return a.IsNull();

            return new Predicate((sql, param) => sql.Append(a, param).Append(" = ").Append(b, param));
        }

        public static Predicate NotEquals(this IDbExpression a, IDbExpression b)
        {
            if (a == null)
                return b.IsNotNull();

            if (b == null)
                return a.IsNotNull();

            return new Predicate((sql, param) => sql.Append(a, param).Append(" <> ").Append(b, param));
        }

        public static Predicate IsNull(this IDbExpression a)
        {
            return new Predicate((sql, param) => sql.Append(a, param).Append(" IS NULL"));
        }

        public static Predicate IsNotNull(this IDbExpression a)
        {
            return new Predicate((sql, param) => sql.Append(a, param).Append(" IS NOT NULL"));
        }

        public static Predicate GreatThan(this IDbExpression a, IDbExpression b)
        {
            if (a == null && b == null)
                throw new ArgumentNullException("a and b", "can't apply comparison operator with NULL");
            return new Predicate((sql, param) => sql.Append(a, param).Append(" > ").Append(b, param));
        }

        public static Predicate GreatThanOrEquals(this IDbExpression a, IDbExpression b)
        {
            if (a == null && b == null)
                throw new ArgumentNullException("a and b", "can't apply comparison operator with NULL");
            return new Predicate((sql, param) => sql.Append(a, param).Append(" >= ").Append(b, param));
        }

        public static Predicate LessThan(this IDbExpression a, IDbExpression b)
        {
            if (a == null && b == null)
                throw new ArgumentNullException("a and b", "can't apply comparison operator with NULL");
            return new Predicate((sql, param) => sql.Append(a, param).Append(" < ").Append(b, param));
        }

        public static Predicate LessThanOrEquals(this IDbExpression a, IDbExpression b)
        {
            if (a == null && b == null)
                throw new ArgumentNullException("a and b", "can't apply comparison operator with NULL");
            return new Predicate((sql, param) => sql.Append(a, param).Append(" <= ").Append(b, param));
        }

        public static Predicate Like(this String a, string pattern)
        {
            return new Predicate((sql, param) => sql.Append(a, param).Append(" LIKE ").AppendParam(param, pattern));
        }

         
    }
}
