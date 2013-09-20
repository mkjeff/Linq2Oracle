using Linq2Oracle.Expressions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Linq2Oracle
{
    // (sql,selection,c,param)=>;
    using SqlGenerator = Action<SqlContext, string, Closure>;

    public sealed class SelectManyContext<C, T, TResult, _> : QueryContext<C, T, TResult>
        where T : DbEntity
        where C : class,new()
    {
        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        readonly _ _transparentId;

        internal SelectManyContext(IQueryContext originalSource, OracleDB db, _ transparentId, Lazy<Projection> projector, SqlGenerator genSql, Closure closure, C columnDefine)
            : base(db, projector, closure, originalSource, genSql, columnDefine)
        {
            _transparentId = transparentId;
        }

        #region Where
        /// <summary>
        /// SQL WHERE
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public SelectManyContext<C, T, TResult, _> Where(Func<_, SqlBoolean> predicate)
        {
            var filter = predicate(_transparentId);
            if (!filter.IsVaild)
                return this;
            var newC = _closure;
            newC.Filters = new List<SqlBoolean>(_closure.Filters) { filter };
            return new SelectManyContext<C, T, TResult, _>(OriginalSource, _db, _transparentId, _projection, _genSql, newC, ColumnDefine);
        }
        #endregion
        #region Select
        public QueryContext<C, T, TResult> Select(Func<_, C> selector)
        {
            return this;
        }
        #endregion
        #region SelectMany
        public SelectManyContext<C, T, TResult, __> SelectMany<C2, T2, TResult2, __>(Func<C, QueryContext<C2, T2, TResult2>> collectionSelector, Func<_, C2, __> resultSelector)
            where T2 : DbEntity
            where C2 : class, new()
        {
            var innerContext = collectionSelector(base.ColumnDefine);

            var newC = _closure;
            newC.Tables = new List<IQueryContext>(_closure.Tables) { innerContext };
            return new SelectManyContext<C, T, TResult, __>(
                originalSource: OriginalSource,
                db: _db,
                transparentId: resultSelector(_transparentId, innerContext.ColumnDefine),
                projector: _projection,
                genSql: (sql, select, c) =>
                {
                    sql.Append(select).Append(" FROM ").Append(TableName).Append(' ').Append(sql.GetAlias(this));
                    foreach (var table in c.Tables)
                        sql.Append(", (").Append("SELECT *",table).Append(") ").Append(sql.GetAlias(table));
                    sql.AppendWhere(c.Filters).AppendOrder(c.Orderby);
                },
                closure: newC,
                columnDefine: ColumnDefine);
        }
        #endregion
        #region OrderBy(Descending) / ThenBy(Descending)
        public SelectManyContext<C, T, TResult, _> OrderBy(Func<_, IEnumerable<ColumnSortDescription>> keySelector)
        {
            var newList = new List<SortDescription>(_closure.Orderby);
            newList.AddRange(from order in keySelector(_transparentId)
                             where Table<T>.DbColumnMap.ContainsKey(order.ColumnName)
                             select new SortDescription(new ColumnExpression(this, order.ColumnName), order.Descending));

            var newC = _closure;
            newC.Orderby = newList;

            return new SelectManyContext<C, T, TResult, _>(OriginalSource, _db, _transparentId, _projection, _genSql, newC, ColumnDefine);
        }
        public SelectManyContext<C, T, TResult, _> OrderBy(Func<_, IDbExpression> keySelector)
        {
            return OrderBy(keySelector(_transparentId),false);
        }
        public SelectManyContext<C, T, TResult, _> OrderByDescending(Func<_, IDbExpression> keySelector)
        {
            return OrderBy(keySelector(_transparentId), true);
        }

        public SelectManyContext<C, T, TResult, _> ThenBy(Func<_, IDbExpression> keySelector)
        {
            return OrderBy(keySelector(_transparentId),false);
        }
        public SelectManyContext<C, T, TResult, _> ThenByDescending(Func<_, IDbExpression> keySelector)
        {
            return OrderBy(keySelector(_transparentId), true);
        }

        SelectManyContext<C, T, TResult, _> OrderBy(IDbExpression expr, bool desc)
        {
            var newC = _closure;
            newC.Orderby = new List<SortDescription>(_closure.Orderby) { new SortDescription(expr, desc) };
            return new SelectManyContext<C, T, TResult, _>(OriginalSource, _db, _transparentId, _projection, _genSql, newC, ColumnDefine);
        }
        #endregion
    }
}
