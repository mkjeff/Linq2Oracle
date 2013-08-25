using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Linq2Oracle
{
    // (sql,selection,c,param)=>;
    using SqlGenerator = Action<StringBuilder, string, Closure, OracleParameterCollection>;

    public sealed class SelectManyContext<T, C, TResult, _> : QueryContext<T, C, TResult> where T : DbEntity
    {
        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        readonly _ _transparentId;


        internal SelectManyContext(OracleDB db, _ transparentId, Lazy<Projection> projector, SqlGenerator genSql, Closure closure)
            : base(db, projector, genSql, closure)
        {
            _transparentId = transparentId;
        }

        public SelectManyContext<T, C, TResult, _> Where(Func<_, Predicate> predicate)
        {
            var filter = predicate(_transparentId);
            if (!filter.IsVaild)
                return this;
            var newC = _closure;
            newC.Filters = new List<Predicate>(_closure.Filters) { filter };
            return new SelectManyContext<T, C, TResult, _>(_db, _transparentId, _projection, _genSql, newC);
        }

        public QueryContext<T, C, TResult> Select(Func<_, C> selector)
        {
            return this;
        }

        #region SelectMany
        public SelectManyContext<T, C, TResult, __> SelectMany<T2, C2, TResult2, __>(Func<C, QueryContext<T2, C2, TResult2>> collectionSelector, Func<_, C2, __> resultSelector) where T2 : DbEntity
        {
            var innerContext = collectionSelector(EntityTable<T, C>.ColumnsDefine);

            var newC = _closure;
            newC.Tables = new List<IQueryContext>(_closure.Tables) { innerContext };

            return new SelectManyContext<T, C, TResult, __>(
                db: _db,
                transparentId: resultSelector(_transparentId, EntityTable<T2, C2>.ColumnsDefine),
                projector: _projection,
                genSql: (sql, select, c, p) =>
                {
                    int index = sql.Length;
                    sql.Append("SELECT ").Append(select).Append(" FROM ").Append(Table<T>.TableName).Append(" t0");
                    int i = 0;
                    foreach (var table in c.Tables.Skip(1))
                    {
                        sql.Append(",(");
                        table.GenInnerSql(sql, p, "t0.*");
                        sql.Append(")t").Append(++i);
                    }
                    sql.AppendWhere(p, c.Filters).AppendOrder(c.Orderby).MappingAlias(index, c.Tables);
                },
                closure: newC);
        }
        #endregion


        #region OrderBy(Descending) / ThenBy(Descending)
        public SelectManyContext<T, C, TResult, _> OrderBy(Func<_, IEnumerable<ColumnSortDescription>> keySelector)
        {
            var newC = _closure;
            newC.Orderby = new List<SortDescription>(EnumerableEx.Concat(
                _closure.Orderby,
                from order in keySelector(_transparentId)
                where Table<T>.DbColumnMap.ContainsKey(order.ColumnName)
                select new SortDescription("t0." + order.ColumnName, order.Descending)));
            return new SelectManyContext<T, C, TResult, _>(_db, _transparentId, _projection, _genSql, newC);
        }
        public SelectManyContext<T, C, TResult, _> OrderBy(Func<_, DbExpression> keySelector)
        {
            return OrderBy(keySelector(_transparentId).Expression);
        }
        public SelectManyContext<T, C, TResult, _> OrderByDescending(Func<_, DbExpression> keySelector)
        {
            return OrderBy(keySelector(_transparentId).Expression, true);
        }

        public SelectManyContext<T, C, TResult, _> ThenBy(Func<_, DbExpression> keySelector)
        {
            return OrderBy(keySelector(_transparentId).Expression);
        }
        public SelectManyContext<T, C, TResult, _> ThenByDescending(Func<_, DbExpression> keySelector)
        {
            return OrderBy(keySelector(_transparentId).Expression, true);
        }

        SelectManyContext<T, C, TResult, _> OrderBy(string expr, bool desc = false)
        {
            var newC = _closure;
            newC.Orderby = new List<SortDescription>(_closure.Orderby) { new SortDescription(expr, desc) };
            return new SelectManyContext<T, C, TResult, _>(_db, _transparentId, _projection, _genSql, newC);
        }
        #endregion
    }
}
