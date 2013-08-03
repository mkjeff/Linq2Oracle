using System;
using Oracle.ManagedDataAccess.Client;
using System.Text;

namespace Linq2Oracle {
    public sealed class Predicate {
        readonly bool localPredicate;
        readonly Action<StringBuilder, OracleParameterCollection> _builder;

        internal bool IsVaild { get { return localPredicate && _builder != null; } }

        internal Predicate(Action<StringBuilder, OracleParameterCollection> gen) {
            localPredicate = true;
            _builder = gen;
        }

        internal Predicate(bool predicate) {
            localPredicate = predicate;
            _builder = null;
        }

        internal void Build(StringBuilder sql, OracleParameterCollection parameters) {
            if (_builder != null)
                _builder(sql, parameters);
        }

        public static implicit operator Predicate(bool boolean) {
            return new Predicate(boolean);
        }

        public static Predicate operator |(Predicate left, Predicate right) {
            bool l = left.IsVaild, r = right.IsVaild;
            if (!(l || r))
                return new Predicate(false);
            if (l && r)
                return new Predicate((sql, param) => {
                    sql.Append("(");
                    left.Build(sql, param);
                    sql.Append(" OR ");
                    right.Build(sql, param);
                    sql.Append(")");
                });
            if (l)
                return left;
            return right;
        }

        public static Predicate operator &(Predicate left, Predicate right) {
            bool l = left.IsVaild, r = right.IsVaild;
            if (!(l || r))
                return new Predicate(false);
            if (l && r)
                return new Predicate((sql, param) => {
                    sql.Append("(");
                    left.Build(sql, param);
                    sql.Append(" AND ");
                    right.Build(sql, param);
                    sql.Append(")");
                });
            if (r)
                return right;
            return left;
        }

        public static Predicate operator !(Predicate a) {
            if (a.IsVaild)
                return new Predicate((sql, param) => {
                    sql.Append("NOT (");
                    a.Build(sql, param);
                    sql.Append(")");
                });
            return new Predicate(!a.localPredicate);
        }

        public static bool operator false(Predicate a) {
            // Used by operator && (7.11.2)
            // x && y --> T.false(x) ? x : T.&(x,y)
            return !a.localPredicate;
        }

        public static bool operator true(Predicate a) {
            // Used by operator || (7.11.2)
            // x || y --> T.true(x) ? x : T.|(x,y)
            return false;
        }
    }
}
