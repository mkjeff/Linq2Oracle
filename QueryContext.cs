using Linq2Oracle.Expressions;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;

namespace Linq2Oracle
{
    // (sql,selection,c)=>;
    using SqlGenerator = Action<SqlContext, string, Closure>;

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T">Table Entity Type</typeparam>
    /// <typeparam name="C">Table Column Definition Type</typeparam>
    /// <typeparam name="TResult">Projection Result Type</typeparam>
    [DebuggerDisplay("查詢 {typeof(T).Name,nq}")]
    public class QueryContext<C, T, TResult> : IQueryContext, IQueryContext<TResult>
        where T : DbEntity
        where C : class,new()
    {
        #region Private Member
        internal readonly C ColumnDefine;

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        internal readonly SqlGenerator _genSql;

        internal readonly Lazy<Projection> _projection;

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        IEnumerable<TResult> _data;

        internal readonly Closure _closure;

        object DebugInfo
        {
            get
            {
                var param = new OracleCommand().Parameters;
                var sql = new SqlContext(new StringBuilder(), param);
                _genSql(sql, "SELECT " + (_closure.Distinct ? "DISTINCT " : string.Empty) + _projection.Value.SelectSql, _closure);
                sql.AppendForUpdate<T, TResult>(_closure.ForUpdate);
                return new
                {
                    SQL = sql.ToString(),
                    QL_PARAM = param
                };
            }
        }
        #endregion
        #region Constructors
        internal QueryContext(Lazy<Projection> projector, Closure closure, SqlGenerator genSql = null, C columnDefine = null)
        {
            _projection = projector;
            _closure = closure;

            if (closure.OriginalSource == null)
                _closure.OriginalSource = this;

            _genSql = genSql ?? ((sql, select, c) =>
            {
                sql.Append(select).MappingAlias(this)
                    .Append(" FROM ").Append(TableName).Append(' ').Append(sql.GetAlias(this))
                    .AppendWhere(c.Filters)
                    .AppendOrder(c.Orderby);
            });

            ColumnDefine = columnDefine ?? ColumnExpressionBuilder<T, C>.Create(this);
            _data = EnumerableEx.Using(() => Db.CreateCommand(), cmd =>
            {
                var sql = new SqlContext(new StringBuilder(128), cmd.Parameters);
                string select = _projection.Value.SelectSql;
                if (_projection.Value.IsProjection && _closure.Distinct)
                    select = "DISTINCT " + select;
                _genSql(sql, "SELECT " + select, _closure);
                sql.AppendForUpdate<T, TResult>(_closure.ForUpdate);
                cmd.CommandText = sql.ToString();
                return EnumerableEx.Using(() => Db.ExecuteReader(cmd), GetResult);
            });
        }

        IEnumerable<TResult> GetResult(OracleDataReader reader)
        {
            if (typeof(T) == typeof(TResult))
                return (IEnumerable<TResult>)ReadIdentityEntities(reader);

            if (_projection.Value.IsProjection)
                return ReadProjectionResult(reader, (Func<OracleDataReader, TResult>)_projection.Value.Projector);

            return ReadConvertedEntities(reader, (Func<T, TResult>)_projection.Value.Projector);
        }

        static IEnumerable<T> ReadIdentityEntities(OracleDataReader reader)
        {
            while (reader.Read())
                yield return TableReader<T>.Read(reader);
        }

        static IEnumerable<TResult> ReadProjectionResult(OracleDataReader reader, Func<OracleDataReader, TResult> projector)
        {
            while (reader.Read())
                yield return projector(reader);
        }

        static IEnumerable<TResult> ReadConvertedEntities(OracleDataReader reader, Func<T, TResult> converter)
        {
            while (reader.Read())
                yield return converter(TableReader<T>.Read(reader));
        }
        #endregion
        #region Single(OrDefault)
        public TResult Single()
        {
            return this.AsEnumerable().Single();
        }
        public TResult Single(Func<C, SqlBoolean> predicate)
        {
            return this.Where(predicate).Single();
        }
        public TResult SingleOrDefault()
        {
            return this.AsEnumerable().SingleOrDefault();
        }
        public TResult SingleOrDefault(Func<C, SqlBoolean> predicate)
        {
            return this.Where(predicate).SingleOrDefault();
        }
        #endregion
        #region First(OrDefault)
        public TResult First()
        {
            return this.Take(1).AsEnumerable().First();
        }
        public TResult First(Func<C, SqlBoolean> predicate)
        {
            return this.Where(predicate).Take(1).First();
        }

        public TResult FirstOrDefault()
        {
            return this.Take(1).AsEnumerable().FirstOrDefault();
        }
        public TResult FirstOrDefault(Func<C, SqlBoolean> predicate)
        {
            return this.Where(predicate).Take(1).FirstOrDefault();
        }
        #endregion
        #region Skip / Take / TakeByPage / TakeBySum
        public QueryContext<C, T, TResult> Skip(int count)
        {
            if (count <= 0)
                return this;
            var newC = _closure;
            newC.Filters = EmptyList<SqlBoolean>.Instance;
            newC.Orderby = EmptyList<SortDescription>.Instance;
            return new QueryContext<C, T, TResult>(_projection, newC, (sql, select, c) =>
                {
                    // SELECT [select] 
                    // FROM (SELECT a.*, ROWNUM AS rn 
                    //       FROM (sql) a 
                    //       ) t0 
                    // WHERE t0.rn > [count] 
                    // [AND WHERE ..] 
                    // [ORDER BY]
                    sql.Append(select).Append(" FROM (SELECT a.* ,ROWNUM AS rn FROM (");
                    _genSql(sql, "SELECT t0.*", _closure);
                    sql.Append(")a )t0 WHERE t0.rn > ").AppendParam(count);

                    if (c.Filters.Any())
                        foreach (var filter in c.Filters)
                            sql.Append(" AND ").Append(filter);
                    sql.AppendOrder(c.Orderby);
                }, ColumnDefine);
        }

        public QueryContext<C, T, TResult> Take(int count)
        {
            if (count < 0)
                return this;
            var newC = _closure;
            newC.Filters = EmptyList<SqlBoolean>.Instance;
            newC.Orderby = EmptyList<SortDescription>.Instance;
            return new QueryContext<C, T, TResult>(_projection, newC, (sql, select, c) =>
                {
                    if (c.Filters.Any() || c.Orderby.Any())
                    {
                        // SELECT [select] 
                        // FROM (SELECT * 
                        //       FROM (sql) t0 
                        //       WHERE ROWNUM <= [count]
                        //       ) t0
                        // WHERE .. 
                        // ORDER BY ..
                        sql.Append(select).Append(" FROM (SELECT * FROM (");
                        _genSql(sql, "SELECT t0.*", _closure);
                        sql.Append(") t0 WHERE ROWNUM <= ").AppendParam(count)
                            .Append(")t0")
                            .AppendWhere(c.Filters)
                            .AppendOrder(c.Orderby);
                    }
                    else
                    {
                        // SELECT [select] 
                        // FROM (sql) t0 
                        // WHERE ROWNUM <= [count]
                        sql.Append(select).Append(" FROM (");
                        _genSql(sql, "SELECT t0.*", _closure);
                        sql.Append(") t0 WHERE ROWNUM <= ").AppendParam(count);
                    }
                    //sql.MappingAlias(i, c.Tables.First());
                }, ColumnDefine);
        }

        public QueryContext<C, T, TResult> TakeByPage(int pageNo, int pageSize)
        {
            int skip = (pageNo - 1) * pageSize;
            var newC = _closure;
            newC.Filters = EmptyList<SqlBoolean>.Instance;
            newC.Orderby = EmptyList<SortDescription>.Instance;
            return new QueryContext<C, T, TResult>(_projection, newC, (sql, select, c) =>
            {
                if (c.Filters.Any() || c.Orderby.Any())
                {
                    // [select]
                    // FROM (SELECT * 
                    //       FROM (SELECT a.* , ROWNUM AS rn 
                    //             FROM (sql) a 
                    //             ) t0 
                    //       WHERE t0.rn > [skip] AND ROWNUM <= [pageSize]
                    //       ) t0 
                    // WHERE ..
                    // ORDER BY ..
                    sql.Append(select).Append(" FROM (SELECT * FROM (SELECT a.* , ROWNUM AS rn FROM (");
                    _genSql(sql, "SELECT t0.*", _closure);
                    sql.Append(") a )t0 WHERE t0.rn > ").AppendParam(skip)
                        .Append(" AND ROWNUM <= ").AppendParam(pageSize)
                        .Append(") t0")
                        .AppendWhere(c.Filters)
                        .AppendOrder(c.Orderby);
                }
                else
                {
                    // [select] 
                    // FROM (SELECT a.* , ROWNUM AS rn 
                    //       FROM (sql) a 
                    //      ) t0 
                    // WHERE t0.rn > [skip] AND ROWNUM <= [pageSize]
                    sql.Append(select)
                        .Append(" FROM (SELECT a.* ,ROWNUM AS rn FROM (");

                    _genSql(sql, "SELECT t0.*", _closure);

                    sql.Append(") a ) t0 WHERE t0.rn > ").AppendParam(skip)
                        .Append(" AND ROWNUM <= ").AppendParam(pageSize);
                }
            }, ColumnDefine);
        }

        public QueryContext<C, T, TResult> TakeBySum<NUM>(Func<C, IDbNumber> sumBy, Func<C, IDbExpression> partitionBy, long sum) where NUM : struct
        {
            if (sum < 0)
                return this;
            var newC = _closure;
            newC.Filters = EmptyList<SqlBoolean>.Instance;
            newC.Orderby = EmptyList<SortDescription>.Instance;
            return new QueryContext<C, T, TResult>(_projection, newC, (sql, select, c) =>
            {
                if (c.Filters.Any() || c.Orderby.Any())
                {
                    // [select] 
                    // FROM (SELECT * 
                    //       FROM (sql) t0 
                    //       WHERE t0.accSum <= [sum]
                    //       )t0 
                    // WHERE .. 
                    // ORDER BY ..
                    sql.Append(select)
                        .Append(" FROM (SELECT t0.*, SUM(")
                            .Append(sumBy(ColumnDefine))
                            .Append(") OVER(PARTITION BY ")
                            .Append(partitionBy(ColumnDefine)).Append(' ')
                            .AppendOrder(_closure.Orderby)
                            .Append(" ROWS UNBOUNDED PRECEDING) AS accSum").ToString();

                    _genSql(sql, string.Empty, _closure);

                    sql.Append(") t0 WHERE t0.accSum <= ").AppendParam(sum)
                        .Append(") t0")
                        .AppendWhere(c.Filters)
                        .AppendOrder(c.Orderby);
                }
                else
                {
                    // [select]     
                    // FROM (sql) t0 
                    // WHERE t0.accSum <= [sum]
                    sql.Append(select)
                        .Append(" FROM (SELECT t0.*, SUM(")
                            .Append(sumBy(ColumnDefine))
                        .Append(") OVER(PARTITION BY ")
                            .Append(partitionBy(ColumnDefine)).Append(' ')
                        .AppendOrder(_closure.Orderby)
                        .Append(" ROWS UNBOUNDED PRECEDING) AS accSum");

                    _genSql(sql, string.Empty, _closure);

                    sql.Append(") t0 WHERE t0.accSum <= ").AppendParam(sum);
                }
            }, ColumnDefine);
        }
        #endregion
        #region OrderBy(Descending) / ThenBy(Descending)
        /// <summary>
        /// SQL ORDER BY
        /// </summary>
        /// <param name="keySelector"></param>
        /// <returns></returns>
        public QueryContext<C, T, TResult> OrderBy(Func<C, IEnumerable<ColumnSortDescription>> keySelector)
        {
            var newList = _closure.Orderby.ToList();
            newList.AddRange(from order in keySelector(ColumnDefine)
                             where Table<T>.DbColumnMap.ContainsKey(order.ColumnName)
                             select new SortDescription(new ColumnExpression(this, order.ColumnName), order.Descending));

            var newC = _closure;
            newC.Orderby = newList;
            return new QueryContext<C, T, TResult>(_projection, newC, _genSql, ColumnDefine);
        }

        /// <summary>
        /// SQL ORDER BY
        /// </summary>
        /// <param name="keySelector"></param>
        /// <returns></returns>
        public QueryContext<C, T, TResult> OrderBy(Func<C, IDbExpression> keySelector)
        {
            return _OrderBy(keySelector(ColumnDefine));
        }

        /// <summary>
        /// SQL ORDER BY DESC
        /// </summary>
        /// <param name="keySelector"></param>
        /// <returns></returns>
        public QueryContext<C, T, TResult> OrderByDescending(Func<C, IDbExpression> keySelector)
        {
            return _OrderBy(keySelector(ColumnDefine), true);
        }

        /// <summary>
        /// SQL ORDER BY
        /// </summary>
        /// <param name="keySelector"></param>
        /// <returns></returns>
        public QueryContext<C, T, TResult> ThenBy(Func<C, IDbExpression> keySelector)
        {
            return _OrderBy(keySelector(ColumnDefine));
        }

        /// <summary>
        /// SQL ORDER BY DESC
        /// </summary>
        /// <param name="keySelector"></param>
        /// <returns></returns>
        public QueryContext<C, T, TResult> ThenByDescending(Func<C, IDbExpression> keySelector)
        {
            return _OrderBy(keySelector(ColumnDefine), true);
        }

        QueryContext<C, T, TResult> _OrderBy(IDbExpression expr, bool desc = false)
        {
            var newC = _closure;
            newC.Orderby = new List<SortDescription>(_closure.Orderby) { new SortDescription(expr, desc) };
            return new QueryContext<C, T, TResult>(_projection, newC, _genSql, ColumnDefine);
        }
        #endregion
        #region Where
        /// <summary>
        /// SQL WHERE
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public QueryContext<C, T, TResult> Where(Func<C, SqlBoolean> predicate)
        {
            var filter = predicate(ColumnDefine);
            if (!filter.IsVaild)
                return this;
            var newC = _closure;
            newC.Filters = new List<SqlBoolean>(_closure.Filters) { filter };
            return new QueryContext<C, T, TResult>(_projection, newC, _genSql, ColumnDefine);
        }
        #endregion
        #region Select
        public QueryContext<C, T, TResult> Select(Func<C, C> selector)
        {
            return this;
        }

        /// <summary>
        /// SQL Projection
        /// </summary>
        /// <typeparam name="TR"></typeparam>
        /// <param name="selector"></param>
        /// <param name="file"></param>
        /// <param name="line"></param>
        /// <returns></returns>
        public QueryContext<C, T, TR> Select<TR>(Expression<Func<T, TR>> selector, [CallerFilePath]string file = "", [CallerLineNumber]int line = 0)
        {
            return new QueryContext<C, T, TR>(new Lazy<Projection>(() => Projection.Create(selector, file, line)), _closure, _genSql, ColumnDefine);
        }
        #endregion
        #region SelectMany
        public SelectManyContext<C, T, TResult, _> SelectMany<C2, T2, TResult2, _>(Func<C, QueryContext<C2, T2, TResult2>> collectionSelector, Func<C, C2, _> resultSelector)
            where T2 : DbEntity
            where C2 : class,new()
        {
            var innerContext = collectionSelector(ColumnDefine);

            var newC = _closure;
            newC.Tables = new List<IQueryContext>(_closure.Tables) { innerContext };

            return new SelectManyContext<C, T, TResult, _>(
                originalSource: OriginalSource,
                transparentId: resultSelector(ColumnDefine, innerContext.ColumnDefine),
                projector: _projection,
                genSql: (sql, select, c) =>
                {
                    sql.Append(select).MappingAlias(this).Append(" FROM ").Append(TableName).Append(' ').Append(sql.GetAlias(this));
                    foreach (var table in c.Tables)
                        sql.Append(", (").Append("*", table).Append(") ").Append(sql.GetAlias(table));
                    sql.AppendWhere(c.Filters).AppendOrder(c.Orderby);
                },
                closure: newC,
                columnDefine: ColumnDefine);
        }
        #endregion
        #region Distinct
        public QueryContext<C, T, TResult> Distinct()
        {
            var newC = _closure;
            newC.Distinct = true;
            return new QueryContext<C, T, TResult>(_projection, newC, _genSql, ColumnDefine);
        }
        #endregion
        #region GroupBy
        /// <summary>
        /// from x in Xs 
        /// group x by key (into g)
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="keySelector"></param>
        /// <returns></returns>
        public GroupingContextCollection<C, T, TKey, TResult> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector, [CallerFilePath]string file = "", [CallerLineNumber]int line = 0)
        {
            var newC = _closure;
            newC.Orderby = EmptyList<SortDescription>.Instance;
            return new GroupingContextCollection<C, T, TKey, TResult>(
                new QueryContext<C, T, TResult>(_projection, newC, _genSql, ColumnDefine),
                keySelector);
        }

        /// <summary>
        /// from x in Xs
        /// group new{...} by key (into g)
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TR"></typeparam>
        /// <param name="keySelector"></param>
        /// <param name="elementSelector"></param>
        /// <returns></returns>
        [Obsolete("Not support group by with element selector", true)]
        public GroupingContextCollection<C, T, TKey, TResult> GroupBy<TKey, TR>(Expression<Func<T, TKey>> keySelector, Expression<Func<T, TR>> elementSelector)
        {
            throw new DalException(DbErrorCode.E_DB_NOT_SUPPORT_OPERATOR, "不支援 group " + elementSelector.ToString() + " by ...");
        }
        #endregion
        #region Intersect / Except / Union /Contact
        QueryContext<C, T, TResult> _SetOperator(QueryContext<C, T, TResult> other, string op)
        {
            var thisC = _closure; thisC.Orderby = EmptyList<SortDescription>.Instance;
            var otherC = other._closure; otherC.Orderby = EmptyList<SortDescription>.Instance;
            return new QueryContext<C, T, TResult>(projector: _projection, closure: new Closure
                {
                    Filters = EmptyList<SqlBoolean>.Instance,
                    Orderby = EmptyList<SortDescription>.Instance,
                    Tables = EmptyList<IQueryContext>.Instance,
                }, genSql: (sql, select, c) =>
                {
                    sql.Append(select).Append(" FROM (");
                    this._genSql(sql, "SELECT t0.*", thisC);
                    sql.Append(op);
                    other._genSql(sql, "SELECT t0.*", otherC);
                    sql.Append(") t0")
                        .AppendWhere(c.Filters)
                        .AppendOrder(c.Orderby);
                }, columnDefine: ColumnDefine);
        }

        /// <summary>
        /// SQL INTERSECT
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public QueryContext<C, T, TResult> Intersect(QueryContext<C, T, TResult> other)
        {
            return _SetOperator(other, " INTERSECT ");
        }

        /// <summary>
        /// SQL MINUS
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public QueryContext<C, T, TResult> Except(QueryContext<C, T, TResult> other)
        {
            return _SetOperator(other, " MINUS ");
        }

        /// <summary>
        /// SQL UNION
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public QueryContext<C, T, TResult> Union(QueryContext<C, T, TResult> other)
        {
            return _SetOperator(other, " UNION ");
        }

        /// <summary>
        /// SQL UNION ALL
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public QueryContext<C, T, TResult> Concat(QueryContext<C, T, TResult> other)
        {
            return _SetOperator(other, " UNION ALL ");
        }
        #endregion
        #region Max / Min / Sum / Average
        #region Max / Min for String
        /// <summary>
        /// SQL MAX Function
        /// </summary>
        /// <param name="selector"></param>
        /// <returns></returns>
        public Expressions.DbString Max(Func<C, Expressions.DbString> selector)
        {
            var exprGen = Function.Call("MAX", selector(ColumnDefine));
            return new Expressions.DbString(() => (string)_AggregateFunction(exprGen),
                sqlBuilder: _AggregateFunctionExpression(exprGen));
        }

        /// <summary>
        /// SQL MIN Function
        /// </summary>
        /// <param name="selector"></param>
        /// <returns></returns>
        public Expressions.DbString Min(Func<C, Expressions.DbString> selector)
        {
            var exprGen = Function.Call("MIN", selector(ColumnDefine));
            return new Expressions.DbString(() => (string)_AggregateFunction(exprGen),
                sqlBuilder: _AggregateFunctionExpression(exprGen));
        }
        #endregion
        #region Max / Min for Number
        /// <summary>
        /// SQL MAX Function
        /// </summary>
        /// <typeparam name="TNumber"></typeparam>
        /// <param name="selector"></param>
        /// <returns></returns>
        public NullableDbNumber Max<TNumber>(Func<C, TNumber> selector) where TNumber : IDbNumber
        {
            var exprGen = Function.Call("MAX", selector(ColumnDefine));
            return new NullableDbNumber(
                valueProvider: () => (decimal?)_AggregateFunction(exprGen),
                sqlBuilder: _AggregateFunctionExpression(exprGen));
        }

        /// <summary>
        /// SQL MAX Function
        /// </summary>
        /// <typeparam name="TNumber"></typeparam>
        /// <param name="selector"></param>
        /// <returns></returns>
        public NullableDbNumber Min<TNumber>(Func<C, TNumber> selector) where TNumber : IDbNumber
        {
            var exprGen = Function.Call("MIN", selector(ColumnDefine));
            return new NullableDbNumber(
                valueProvider: () => (decimal?)_AggregateFunction(exprGen),
                sqlBuilder: _AggregateFunctionExpression(exprGen));
        }
        #endregion
        #region Max / Min for DateTime
        /// <summary>
        /// SQL MAX Function
        /// </summary>
        /// <typeparam name="TNumber"></typeparam>
        /// <param name="selector"></param>
        /// <returns></returns>
        public NullableDbDateTime Max(Func<C, Expressions.DbDateTime> selector)
        {
            var exprGen = Function.Call("MAX", selector(ColumnDefine));
            return new NullableDbDateTime(() => (System.DateTime?)_AggregateFunction(exprGen), _AggregateFunctionExpression(exprGen));
        }

        /// <summary>
        /// SQL MAX Function
        /// </summary>
        /// <typeparam name="TNumber"></typeparam>
        /// <param name="selector"></param>
        /// <returns></returns>
        public NullableDbDateTime Min(Func<C, Expressions.DbDateTime> selector)
        {
            var exprGen = Function.Call("MIN", selector(ColumnDefine));
            return new NullableDbDateTime(() => (System.DateTime?)_AggregateFunction(exprGen), _AggregateFunctionExpression(exprGen));
        }

        /// <summary>
        /// SQL MAX Function
        /// </summary>
        /// <typeparam name="TNumber"></typeparam>
        /// <param name="selector"></param>
        /// <returns></returns>
        public NullableDbDateTime Max(Func<C, NullableDbDateTime> selector)
        {
            var exprGen = Function.Call("MAX", selector(ColumnDefine));
            return new NullableDbDateTime(() => (System.DateTime?)_AggregateFunction(exprGen), _AggregateFunctionExpression(exprGen));
        }

        /// <summary>
        /// SQL MAX Function
        /// </summary>
        /// <typeparam name="TNumber"></typeparam>
        /// <param name="selector"></param>
        /// <returns></returns>
        public NullableDbDateTime Min(Func<C, NullableDbDateTime> selector)
        {
            var exprGen = Function.Call("MIN", selector(ColumnDefine));
            return new NullableDbDateTime(() => (System.DateTime?)_AggregateFunction(exprGen), _AggregateFunctionExpression(exprGen));
        }
        #endregion
        #region Max / Min for TimeSpan
        /// <summary>
        /// SQL MAX Function
        /// </summary>
        /// <typeparam name="TNumber"></typeparam>
        /// <param name="selector"></param>
        /// <returns></returns>
        public NullableDbTimeSpan Max(Func<C, Expressions.DbTimeSpan> selector)
        {
            var exprGen = Function.Call("MAX", selector(ColumnDefine));
            return new NullableDbTimeSpan(() => (System.TimeSpan?)_AggregateFunction(exprGen), _AggregateFunctionExpression(exprGen));
        }

        /// <summary>
        /// SQL MAX Function
        /// </summary>
        /// <typeparam name="TNumber"></typeparam>
        /// <param name="selector"></param>
        /// <returns></returns>
        public NullableDbTimeSpan Min(Func<C, Expressions.DbTimeSpan> selector)
        {
            var exprGen = Function.Call("MIN", selector(ColumnDefine));
            return new NullableDbTimeSpan(() => (System.TimeSpan?)_AggregateFunction(exprGen), _AggregateFunctionExpression(exprGen));
        }

        /// <summary>
        /// SQL MAX Function
        /// </summary>
        /// <typeparam name="TNumber"></typeparam>
        /// <param name="selector"></param>
        /// <returns></returns>
        public NullableDbTimeSpan Max(Func<C, NullableDbTimeSpan> selector)
        {
            var exprGen = Function.Call("MAX", selector(ColumnDefine));
            return new NullableDbTimeSpan(() => (System.TimeSpan?)_AggregateFunction(exprGen), _AggregateFunctionExpression(exprGen));
        }

        /// <summary>
        /// SQL MAX Function
        /// </summary>
        /// <typeparam name="TNumber"></typeparam>
        /// <param name="selector"></param>
        /// <returns></returns>
        public NullableDbTimeSpan Min(Func<C, NullableDbTimeSpan> selector)
        {
            var exprGen = Function.Call("MIN", selector(ColumnDefine));
            return new NullableDbTimeSpan(() => (System.TimeSpan?)_AggregateFunction(exprGen), _AggregateFunctionExpression(exprGen));
        }
        #endregion
        #region Sum
        /// <summary>
        /// SQL SUM Function
        /// </summary>
        /// <typeparam name="TNumber"></typeparam>
        /// <param name="selector"></param>
        /// <returns></returns>
        public NullableDbNumber Sum<TNumber>(Func<C, TNumber> selector) where TNumber : IDbNumber
        {
            var exprGen = Function.Call("SUM", selector(ColumnDefine));
            return new NullableDbNumber(
                valueProvider: () => (decimal?)_AggregateFunction(exprGen),
                sqlBuilder: _AggregateFunctionExpression(exprGen));
        }
        #endregion
        #region Average
        /// <summary>
        /// SQL AVG Function
        /// </summary>
        /// <typeparam name="TNumber"></typeparam>
        /// <param name="selector"></param>
        /// <returns></returns>
        public NullableDbNumber Average<TNumber>(Func<C, TNumber> selector) where TNumber : IDbNumber
        {
            Action<SqlContext> exprGen = sql => sql.Append("ROUND(AVG(").Append(selector(ColumnDefine)).Append("),25)");
            return new NullableDbNumber(
                valueProvider: () => (decimal?)_AggregateFunction(exprGen),
                sqlBuilder: _AggregateFunctionExpression(exprGen));
        }
        #endregion

        object _AggregateFunction(Action<SqlContext> exprGen)
        {
            var cc = _closure;
            cc.Orderby = EmptyList<SortDescription>.Instance;
            using (var cmd = Db.CreateCommand())
            {
                var sql = new SqlContext(new StringBuilder(), cmd.Parameters);
                sql.Append("SELECT ").Append(exprGen);
                _genSql(sql, string.Empty, cc);
                cmd.CommandText = sql.ToString();
                var result = Db.ExecuteScalar(cmd);
                return result == DBNull.Value ? null : result;
            }
        }

        Action<SqlContext> _AggregateFunctionExpression(Action<SqlContext> exprGen)
        {
            return sql =>
            {
                sql.Append('(');
                var cc = _closure;
                cc.Orderby = EmptyList<SortDescription>.Instance;
                sql.Append("SELECT ").Append(exprGen);
                _genSql(sql, string.Empty, cc);
                sql.Append(')');
            };
        }
        #endregion
        #region Count / LongCount
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        DbNumber _Count()
        {
            var cc = _closure; cc.Orderby = EmptyList<SortDescription>.Instance;
            var selection = _projection.Value.SelectSql;

            return new DbNumber(
                valueProvider: () =>
                {
                    using (var cmd = Db.CreateCommand())
                    {
                        var sql = new SqlContext(new StringBuilder(), cmd.Parameters);
                        if (_closure.Distinct)
                        {
                            if (selection.IndexOf(',') == -1)
                            {
                                // select single column
                                _genSql(sql, "SELECT COUNT(DISTINCT " + selection + ")", cc);
                            }
                            else
                            {
                                sql.Append("SELECT COUNT(*) FROM (");
                                _genSql(sql, "SELECT DISTINCT " + selection, cc);
                                sql.Append(")");
                            }
                        }
                        else
                        {
                            _genSql(sql, "SELECT COUNT(*)", cc);
                        }
                        cmd.CommandText = sql.ToString();
                        return (decimal)Db.ExecuteScalar(cmd);
                    }
                },
                sqlBuilder: sql =>
                {
                    sql.Append('(');
                    if (_closure.Distinct)
                    {
                        if (selection.IndexOf(',') == -1)
                        {
                            // select single column
                            _genSql(sql, "SELECT COUNT(DISTINCT " + selection + ")", cc);
                        }
                        else
                        {
                            sql.Append("SELECT COUNT(*) FROM (");
                            _genSql(sql, "SELECT DISTINCT " + selection, cc);
                            sql.Append(")");
                        }
                    }
                    else
                    {
                        _genSql(sql, "SELECT COUNT(*)", cc);
                    }
                    sql.Append(')');
                });
        }

        /// <summary>
        /// SQL COUNT as int
        /// </summary>
        /// <returns></returns>
        public DbNumber Count()
        {
            return _Count();
        }

        /// <summary>
        /// SQL COUNT as int
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public DbNumber Count(Func<C, SqlBoolean> predicate)
        {
            return this.Where(predicate).Count();
        }

        /// <summary>
        /// SQL COUNT as int
        /// </summary>
        /// <returns></returns>
        public DbNumber LongCount()
        {
            return _Count();
        }

        /// <summary>
        /// SQL COUNT as int
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public DbNumber LongCount(Func<C, SqlBoolean> predicate)
        {
            return this.Where(predicate).LongCount();
        }
        #endregion
        #region Any / IsEmpty
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        public BooleanContext Any()
        {
            return new BooleanContext(
                valueProvider: () =>
                {
                    var cc = _closure; cc.Orderby = EmptyList<SortDescription>.Instance;
                    using (var cmd = Db.CreateCommand())
                    {
                        var sql = new SqlContext(new StringBuilder("SELECT CASE WHEN (EXISTS(SELECT NULL FROM ("), cmd.Parameters);
                        _genSql(sql, "SELECT *", cc);
                        sql.Append("))) THEN 1 ELSE 0 END value FROM DUAL");

                        cmd.CommandText = sql.ToString();
                        return (decimal)Db.ExecuteScalar(cmd) == 1;
                    }
                },
                predicate: new SqlBoolean(sql =>
                {
                    sql.Append("EXISTS (");
                    var newC = _closure;
                    newC.Orderby = EmptyList<SortDescription>.Instance;
                    _genSql(sql, "SELECT 1", newC);
                    sql.Append(')');
                }));
        }

        public BooleanContext Any(Func<C, SqlBoolean> predicate)
        {
            return this.Where(predicate).Any();
        }

        public BooleanContext IsEmpty()
        {
            return !Any();
        }
        #endregion
        #region All
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        public bool All(Func<C, SqlBoolean> predicate)
        {
            var filter = predicate(ColumnDefine);
            if (!filter.IsVaild)
                throw new DalException(DbErrorCode.E_DB_SQL_INVAILD, "All條件有誤");
            var cc = _closure; cc.Orderby = EmptyList<SortDescription>.Instance;
            using (var cmd = Db.CreateCommand())
            {
                var sql = new SqlContext(new StringBuilder("SELECT CASE WHEN (NOT EXISTS(SELECT * FROM ("), cmd.Parameters);
                _genSql(sql, "SELECT *", cc);
                sql.Append(") a WHERE NOT (").Append(filter).Append("))) THEN 1 ELSE 0 END value FROM DUAL");
                cmd.CommandText = sql.ToString();
                return (decimal)Db.ExecuteScalar(cmd) == 1;
            }
        }
        #endregion
        #region ForUpdate(Row Lock)
        const int DEFAULT_WAIT = 10;
        public QueryContext<C, T, TResult> ForUpdate()
        {
            return ForUpdate(DEFAULT_WAIT);
        }
        public QueryContext<C, T, TResult> ForUpdate(int waitSec)
        {
            var newClosure = _closure;
            newClosure.ForUpdate = waitSec;
            return new QueryContext<C, T, TResult>(_projection, newClosure, _genSql, ColumnDefine);
        }
        #endregion
        #region IEnumerable<TResult> 成員
        IEnumerator<TResult> IEnumerable<TResult>.GetEnumerator()
        {
            return _data.GetEnumerator();
        }
        #endregion
        #region IEnumerable 成員
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<TResult>)this).GetEnumerator();
        }
        #endregion
        #region IQueryContext 成員
        public IQueryContext OriginalSource { get { return _closure.OriginalSource; } }

        void IQueryContext.GenInnerSql(SqlContext sql, string selection)
        {
            selection = selection ?? _projection.Value.SelectSql;
            var newC = _closure;
            newC.Orderby = EmptyList<SortDescription>.Instance;
            if (newC.Distinct)
                selection = "DISTINCT " + selection;
            _genSql(sql, "SELECT " + selection, newC);
        }

        void IQueryContext.GenBatchSql(SqlContext sql, OracleParameter refParam)
        {
            _data = EnumerableEx.Using(() => refParam, p =>
                    EnumerableEx.Using(() => ((OracleRefCursor)p.Value).GetDataReader(), GetResult));
            string select = _projection.Value.SelectSql;
            if (_projection.Value.IsProjection && _closure.Distinct)
                select = "DISTINCT " + select;
            _genSql(sql, "SELECT " + select, _closure);
            sql.AppendForUpdate<T, TResult>(_closure.ForUpdate);
        }

        public OracleDB Db { get { return _closure.Db; } }

        public string TableName { get { return Table<T>.TableName; } }
        #endregion
    }

    static class ColumnExpressionBuilder<T, C>
        where T : DbEntity
        where C : new()
    {
        static readonly Func<IQueryContext, C> constructor;

        static ColumnExpressionBuilder()
        {
            var properties = from prop in typeof(C).GetProperties()
                             where typeof(IDbExpression).IsAssignableFrom(prop.PropertyType)
                             where Table<T>.DbColumnMap.ContainsKey(prop.Name)
                             select new
                             {
                                 PropertyInfo = prop,
                                 ColumnInfo = Table<T>.DbColumnMap[prop.Name]
                             };

            //var lambda = (IQueryContext query)=> 
            //      new C {
            //          Column1 = SqlExpressionBuilder.Create<Column1Type>(sql => sql.Append(sql.GetAlias(query).Append('.').Append(column1.QuotesColumnName)),
            //          Column2 = ...
            //      };
            var query = LambdaExpression.Parameter(typeof(IQueryContext), "query");
            var lambda = LambdaExpression.Lambda<Func<IQueryContext, C>>(
                body: LambdaExpression.MemberInit(
                    newExpression: LambdaExpression.New(typeof(C)),
                    bindings: from prop in properties
                              let sql = LambdaExpression.Parameter(typeof(SqlContext), "sql")
                              select (MemberBinding)Expression.Bind(
                                    member: prop.PropertyInfo,
                                    expression: Expression.Call(
                                        typeof(SqlExpressionBuilder),
                                        "Create",
                                        new Type[] { prop.PropertyInfo.PropertyType },
                                        LambdaExpression.Lambda<Action<SqlContext>>(
                                            body: LambdaExpression.Call(
                                                LambdaExpression.Call(
                                                    LambdaExpression.Call(sql,
                                                        "Append", null, LambdaExpression.Call(sql, "GetAlias", null, query)),
                                                        "Append", null, LambdaExpression.Constant('.')),
                                                        "Append", null, LambdaExpression.Constant(prop.ColumnInfo.QuotesColumnName)),
                                            parameters: sql
                                        )
                                    )
                              )
                 ),
                 parameters: query);

            constructor = lambda.Compile();
        }

        static public C Create(IQueryContext query)
        {
            //var type = typeof(C);
            //var ColumnsDefine = (C)Activator.CreateInstance(type);
            //foreach (var prop in type.GetProperties())
            //{
            //    DbColumn c;
            //    if (!Table<T>.DbColumnMap.TryGetValue(prop.Name, out c) ||
            //        !typeof(ISqlExpressionBuilder).IsAssignableFrom(prop.PropertyType))
            //        continue;

            //    var value = (ISqlExpressionBuilder)Activator.CreateInstance(prop.PropertyType);
            //    value.Init(c.DbType, sql => sql.Append(sql.GetAlias(query)).Append('.').Append(c.QuotesColumnName));
            //    prop.SetValue(ColumnsDefine, value, null);
            //}
            //return ColumnsDefine;
            return constructor(query);
        }
    }

    /// <summary>
    /// Entity LINQ Queryable Object 
    /// </summary>
    /// <typeparam name="T">Entity Type</typeparam>
    /// <typeparam name="C">This type is used for representation of SQL expression clause in WHERE, ORDER BY and HAVING</typeparam>
    public sealed class EntityTable<T, C> : QueryContext<C, T, T>
        where T : DbEntity
        where C : class,new()
    {
        static EntityTable() { }

        static readonly Lazy<Projection> identityProjection = new Lazy<Projection>(() => Projection.Identity<T>());

        public EntityTable(OracleDB db)
            : base(identityProjection, new Closure
            {
                Db = db,
                Filters = EmptyList<SqlBoolean>.Instance,
                Orderby = EmptyList<SortDescription>.Instance,
                Tables = EmptyList<IQueryContext>.Instance,
            }) { }
    }

    struct Closure
    {
        public OracleDB Db;
        public IQueryContext OriginalSource;
        public IReadOnlyList<IQueryContext> Tables;
        public IReadOnlyList<SqlBoolean> Filters;
        public IReadOnlyList<SortDescription> Orderby;
        public bool Distinct;

        /// <summary>
        /// &lt; 0:SKIP LOCKED
        /// 0: NOWAIT
        /// > 0 : WAIT
        /// </summary>
        public int? ForUpdate;
    }

    sealed class SortDescription
    {
        public IDbExpression Expression { get; private set; }
        public bool Descending { get; private set; }
        public SortDescription(IDbExpression expr, bool desc)
        {
            Expression = expr;
            Descending = desc;
        }
    }

    /// <summary>
    /// Dynamic sorting description 
    /// </summary>
    public struct ColumnSortDescription
    {
        public string ColumnName { get; set; }
        public bool Descending { get; set; }
    }
}
