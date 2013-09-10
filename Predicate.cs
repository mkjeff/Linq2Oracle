using Oracle.ManagedDataAccess.Client;
using System;
using System.Text;
using System.Collections.Generic;

namespace Linq2Oracle.Expressions
{
    public struct SqlBoolean
    {
        readonly Action<SqlContext> _builder;

        internal bool IsVaild { get { return _builder != null; } }

        internal SqlBoolean(Action<SqlContext> sqlBuilder)
        {
            _builder = sqlBuilder;
        }

        internal void Build(SqlContext sql)
        {
            if (_builder != null)
                _builder(sql);
        }

        public static SqlBoolean operator |(SqlBoolean x, SqlBoolean y)
        {
            bool l = x.IsVaild, r = y.IsVaild;
            
            if (l && r) return new SqlBoolean(sql => sql.Append("(").Append(x).Append(" OR ").Append(y).Append(")"));
            if (l) return x;
            if (r) return y;

            return default(SqlBoolean);
        }

        public static SqlBoolean operator &(SqlBoolean x, SqlBoolean y)
        {
            bool l = x.IsVaild, r = y.IsVaild;

            if (l && r) return new SqlBoolean(sql => sql.Append("(").Append(x).Append(" AND ").Append(y).Append(")"));
            if (l) return x;
            if (r) return y;

            return default(SqlBoolean);
        }

        public static SqlBoolean operator !(SqlBoolean x)
        {
            if (x.IsVaild) return new SqlBoolean(sql => sql.Append("NOT (").Append(x).Append(")"));
            return default(SqlBoolean);
        }

        public static bool operator false(SqlBoolean x)
        {
            // Used by operator && (7.11.2)
            // x && y --> T.false(x) ? x : T.&(x,y)
            return !x.IsVaild;
        }

        public static bool operator true(SqlBoolean x)
        {
            // Used by operator || (7.11.2)
            // x || y --> T.true(x) ? x : T.|(x,y)
            return false;
        }
    }

    public struct BooleanContext
    {
        readonly Func<bool> _valueProvider;
        readonly SqlBoolean _predicate;

        internal BooleanContext(Func<bool> valueProvider, SqlBoolean predicate)
        {
            _valueProvider = valueProvider;
            _predicate = predicate;
        }

        public static BooleanContext operator !(BooleanContext x)
        {
            return new BooleanContext(() => !x._valueProvider(), !x._predicate);
        }

        public static implicit operator bool(BooleanContext @this)
        {
            return @this._valueProvider();
        }

        public static implicit operator SqlBoolean(BooleanContext @this)
        {
            return @this._predicate;
        }
    }
}
