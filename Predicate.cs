using Oracle.ManagedDataAccess.Client;
using System;
using System.Text;

namespace Linq2Oracle
{
    /// <summary>
    /// SQL Predicate
    /// </summary>
    public struct Predicate
    {
        public static readonly Predicate Ignore = new Predicate();

        readonly Action<StringBuilder, OracleParameterCollection> _builder;

        internal bool IsVaild { get { return _builder != null; } }

        internal Predicate(Action<StringBuilder, OracleParameterCollection> gen)
        {
            _builder = gen;
        }

        internal void Build(StringBuilder sql, OracleParameterCollection parameters)
        {
            if (_builder != null)
                _builder(sql, parameters);
        }

        public static Predicate operator |(Predicate x, Predicate y)
        {
            bool l = x.IsVaild, r = y.IsVaild;

            if (l && r) return new Predicate((sql, param) =>
            {
                sql.Append("(");
                x.Build(sql, param);
                sql.Append(" OR ");
                y.Build(sql, param);
                sql.Append(")");
            });

            if (l) return x;
            if (r) return y;
            return Ignore;
        }

        public static Predicate operator &(Predicate x, Predicate y)
        {
            bool l = x.IsVaild, r = y.IsVaild;

            if (l && r) return new Predicate((sql, param) =>
            {
                sql.Append("(");
                x.Build(sql, param);
                sql.Append(" AND ");
                y.Build(sql, param);
                sql.Append(")");
            });

            if (l) return x;
            if (r) return y;
            return Ignore;
        }

        public static Predicate operator !(Predicate x)
        {
            if (x.IsVaild) return new Predicate((sql, param) =>
            {
                sql.Append("NOT (");
                x.Build(sql, param);
                sql.Append(")");
            });

            return Ignore;
        }

        public static bool operator false(Predicate x)
        {
            // Used by operator && (7.11.2)
            // x && y --> T.false(x) ? x : T.&(x,y)
            return !x.IsVaild;
        }

        public static bool operator true(Predicate x)
        {
            // Used by operator || (7.11.2)
            // x || y --> T.true(x) ? x : T.|(x,y)
            return false;
        }
    }
}
