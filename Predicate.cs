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

        /// <summary>
        /// Optional SQL predicate when localCondition is true
        /// </summary>
        /// <param name="localCondition"></param>
        /// <returns></returns>
        public Predicate When(bool localCondition)
        {
            return localCondition ? this : new Predicate();
        }

        public static Predicate operator |(Predicate x, Predicate y)
        {
            bool l = x.IsVaild, r = y.IsVaild;
            if (!(l || r))
                return new Predicate();
            if (l && r)
                return new Predicate((sql, param) =>
                {
                    sql.Append("(");
                    x.Build(sql, param);
                    sql.Append(" OR ");
                    y.Build(sql, param);
                    sql.Append(")");
                });
            if (l)
                return x;
            return y;
        }

        public static Predicate operator &(Predicate x, Predicate y)
        {
            bool l = x.IsVaild,
                r = y.IsVaild;
            if (!(l || r))
                return new Predicate();
            if (l && r)
                return new Predicate((sql, param) =>
                {
                    sql.Append("(");
                    x.Build(sql, param);
                    sql.Append(" AND ");
                    y.Build(sql, param);
                    sql.Append(")");
                });
            if (r)
                return y;
            return x;
        }

        public static Predicate operator !(Predicate x)
        {
            if (x.IsVaild)
                return new Predicate((sql, param) =>
                {
                    sql.Append("NOT (");
                    x.Build(sql, param);
                    sql.Append(")");
                });
            return new Predicate();
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
