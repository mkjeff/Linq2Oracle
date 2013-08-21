using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Linq2Oracle.Expressions;

namespace Linq2Oracle
{
    // (sql,selection,c,param)=>;
    using SqlGenerator = Action<StringBuilder, string, Closure, OracleParameterCollection>;

    public sealed class SelectManyContext<C, T, TResult, _> : QueryContext<C, T, TResult> where T : DbEntity
    {
        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        readonly _ _transparentId;


        internal SelectManyContext(OracleDB db, _ transparentId, Lazy<Projection> projector, SqlGenerator genSql, Closure closure)
            : base(db, projector, genSql, closure)
        {
            _transparentId = transparentId;
        }

        public SelectManyContext<C, T, TResult, _> Where(Func<_, Predicate> predicate)
        {
            var filter = predicate(_transparentId);
            if (!filter.IsVaild)
                return this;
            var newC = _closure;
            newC.Filters = _closure.Filters.Concat(EnumerableEx.Return(filter));
            return new SelectManyContext<C, T, TResult, _>(_db, _transparentId, _projection, _genSql, newC);
        }

        public QueryContext<C, T, TResult> Select(Func<_, C> selector)
        {
            return this;
        }

        #region OrderBy(Descending) / ThenBy(Descending)
        //public SelectManyContext<T, C, TResult, _> OrderBy(Func<_, IEnumerable<ColumnSortDescription>> keySelector)
        //{
        //    var newC = _closure;
        //    newC.Orderby = EnumerableEx.Concat(
        //        _closure.Orderby,
        //        from order in keySelector(_transparentId)
        //        where Table<T>.DbColumnMap.ContainsKey(order.ColumnName)
        //        select new SortDescription("t0." + order.ColumnName, order.Descending));
        //    return new SelectManyContext<T, C, TResult, _>(_db, _transparentId, _projection, _genSql, newC);
        //}
        public SelectManyContext<C, T, TResult, _> OrderBy(Func<_, IDbExpression> keySelector)
        {
            return OrderBy(keySelector(_transparentId));
        }
        public SelectManyContext<C, T, TResult, _> OrderByDescending(Func<_, IDbExpression> keySelector)
        {
            return OrderBy(keySelector(_transparentId), true);
        }

        public SelectManyContext<C, T, TResult, _> ThenBy(Func<_, IDbExpression> keySelector)
        {
            return OrderBy(keySelector(_transparentId));
        }
        public SelectManyContext<C, T, TResult, _> ThenByDescending(Func<_, IDbExpression> keySelector)
        {
            return OrderBy(keySelector(_transparentId), true);
        }

        SelectManyContext<C, T, TResult, _> OrderBy(IDbExpression expr, bool desc = false)
        {
            var newC = _closure;
            newC.Orderby = EnumerableEx.Concat(_closure.Orderby, new SortDescription(expr, desc));
            return new SelectManyContext<C, T, TResult, _>(_db, _transparentId, _projection, _genSql, newC);
        }
        #endregion
    }
}
