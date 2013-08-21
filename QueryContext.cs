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
using Linq2Oracle.Expressions;

namespace Linq2Oracle
{
    // (sql,selection,c,param)=>;
    using SqlGenerator = Action<StringBuilder, string, Closure, OracleParameterCollection>;

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T">Table Entity Type</typeparam>
    /// <typeparam name="C">Table Column Definition Type</typeparam>
    /// <typeparam name="TResult">Projection Result Type</typeparam>
    [DebuggerDisplay("查詢 {typeof(T).Name,nq}")]
    public class QueryContext<C, T, TResult> : IQueryContext, IQueryContext<TResult> where T : DbEntity
    {
        #region Private Member

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        protected readonly OracleDB _db;

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        internal readonly SqlGenerator _genSql;

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        internal readonly Lazy<Projection> _projection;

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        IEnumerable<TResult> _data;

        internal readonly Closure _closure;

        object DebugInfo
        {
            get
            {
                var param = new OracleCommand().Parameters;
                var sb = new StringBuilder();
                _genSql(sb, (_closure.Distinct ? "DISTINCT " : string.Empty) + _projection.Value.SelectSql, _closure, param);
                sb.AppendForUpdate<T, TResult>(_closure.ForUpdate);
                return new
                {
                    SQL = sb.ToString(),
                    QL_PARAM = param.Cast<OracleParameter>().Select(p => p.Value).ToArray()
                };
            }
        }
        #endregion
        #region Constructors
        internal QueryContext(OracleDB db, Lazy<Projection> identityProjection)
        {
            _db = db;
            _projection = identityProjection;
            _genSql = (sql, select, c, p) =>
            {
                int i = sql.Length;
                sql.Append("SELECT ").Append(select)
                    .Append(" FROM ").Append(Table<T>.TableName).Append(" t0")
                    .AppendWhere(p, c.Filters)
                    .AppendOrder(c.Orderby)
                    .MappingAlias(i, c.Tables.First());
            };
            _closure = new Closure
            {
                Filters = Enumerable.Empty<Predicate>(),
                Orderby = Enumerable.Empty<SortDescription>(),
                Tables = EnumerableEx.Return((IQueryContext)this)
            };
            _data = GetData();
        }

        QueryContext(OracleDB db, Lazy<Projection> projector, SqlGenerator genSql)
        {
            _db = db;
            _genSql = genSql;
            _closure = new Closure
            {
                Filters = Enumerable.Empty<Predicate>(),
                Orderby = Enumerable.Empty<SortDescription>(),
                Tables = EnumerableEx.Return((IQueryContext)this)
            };
            _projection = projector;
            _data = GetData();
        }

        internal QueryContext(OracleDB db, Lazy<Projection> projector, SqlGenerator genSql, Closure closure)
        {
            _db = db;
            _genSql = genSql;
            _closure = closure;
            _projection = projector;
            _data = GetData();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        IEnumerable<TResult> GetData()
        {
            return EnumerableEx.Using(() => _db.CreateCommand(), cmd =>
            {
                var sql = new StringBuilder(128);
                string select = _projection.Value.SelectSql;
                if (_closure.Distinct)
                    select = "DISTINCT " + select;
                _genSql(sql, select, _closure, cmd.Parameters);
                sql.AppendForUpdate<T, TResult>(_closure.ForUpdate);
                cmd.CommandText = sql.ToString();
                return EnumerableEx.Using(() => _db.ExecuteReader(cmd), reader =>
                     typeof(T) == typeof(TResult) ? (IEnumerable<TResult>)ReadIdentityEntities(reader) :
                     _projection.Value.IsProjection ? ReadProjectionResult(reader, (Func<OracleDataReader, TResult>)_projection.Value.Projector) :
                     ReadConvertedEntities(reader, (Func<T, TResult>)_projection.Value.Projector));
            });
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
        public TResult Single(Func<C, Predicate> predicate)
        {
            return this.Where(predicate).Single();
        }
        public TResult SingleOrDefault()
        {
            return this.AsEnumerable().SingleOrDefault();
        }
        public TResult SingleOrDefault(Func<C, Predicate> predicate)
        {
            return this.Where(predicate).SingleOrDefault();
        }
        #endregion
        #region First(OrDefault)
        public TResult First()
        {
            return this.Take(1).AsEnumerable().First();
        }
        public TResult First(Func<C, Predicate> predicate)
        {
            return this.Where(predicate).Take(1).First();
        }

        public TResult FirstOrDefault()
        {
            return this.Take(1).AsEnumerable().FirstOrDefault();
        }
        public TResult FirstOrDefault(Func<C, Predicate> predicate)
        {
            return this.Where(predicate).Take(1).FirstOrDefault();
        }
        #endregion
        #region Skip / Take
        public QueryContext<C, T, TResult> Skip(int count)
        {
            if (count <= 0)
                return this;
            var newC = _closure;
            newC.Filters = Enumerable.Empty<Predicate>();
            newC.Orderby = Enumerable.Empty<SortDescription>();
            return new QueryContext<C, T, TResult>(
                _db,
                _projection,
                (sql, select, c, p) =>
                {
                    // SELECT [select] FROM (SELECT a.* ,ROWNUM AS rn FROM (sql) a ) t0 WHERE t0.rn > [count] [AND WHERE filter] [ORDER BY]
                    int i = sql.Length;
                    sql.Append("SELECT ").Append(select).Append(" FROM (SELECT a.* ,ROWNUM AS rn FROM (");
                    _genSql(sql, "t0.*", _closure, p);
                    sql.Append(")a )t0 WHERE t0.rn > :").Append(p.Add(p.Count.ToString(), count).ParameterName);

                    if (c.Filters.Any())
                        foreach (var filter in c.Filters)
                        {
                            sql.Append(" AND ");
                            filter.Build(sql, p);
                        }
                    sql.AppendOrder(c.Orderby).MappingAlias(i, c.Tables.First());
                },
                newC);
        }

        public QueryContext<C, T, TResult> Take(int count)
        {
            if (count < 0)
                return this;
            var newC = _closure;
            newC.Filters = Enumerable.Empty<Predicate>();
            newC.Orderby = Enumerable.Empty<SortDescription>();
            return new QueryContext<C, T, TResult>(
                _db,
                _projection,
                (sql, select, c, p) =>
                {
                    // SELECT [select]                FROM (sql) t0 WHERE ROWNUM <= [count]
                    // SELECT [select] FROM (SELECT * FROM (sql) t0 WHERE ROWNUM <= [count])t0 [WHERE .. ORDER BY ..])
                    int i = sql.Length;
                    sql.Append("SELECT ").Append(select).Append(" FROM (");
                    _genSql(sql, "t0.*", _closure, p);
                    sql.Append(") t0 WHERE ROWNUM <= :").Append(p.Add(p.Count.ToString(), count).ParameterName);

                    if (c.Filters.Any() || c.Orderby.Any())
                        sql.Insert(i + 7 + select.Length, " FROM (SELECT *").Append(") t0").AppendWhere(p, c.Filters).AppendOrder(c.Orderby);

                    sql.MappingAlias(i, c.Tables.First());
                },
                newC);
        }
        #endregion
        #region TakeBySum
        public QueryContext<C, T, TResult> TakeBySum<NUM>(Func<C, Number<NUM>> sumBy, Func<C, IDbExpression> partitionBy, long sum) where NUM : struct
        {
            if (sum < 0)
                return this;
            var newC = _closure;
            newC.Filters = Enumerable.Empty<Predicate>();
            newC.Orderby = Enumerable.Empty<SortDescription>();
            return new QueryContext<C, T, TResult>(
                _db,
                _projection,
                (sql, select, c, p) =>
                {
                    // SELECT [select]                FROM (sql) t0 WHERE ROWNUM <= [count]
                    // SELECT [select] FROM (SELECT * FROM (sql) t0 WHERE ROWNUM <= [count])t0 [WHERE .. ORDER BY ..])
                    int i = sql.Length;
                    sql.Append("SELECT ").Append(select).Append(" FROM (");

                    var tmpSelect = new StringBuilder()
                        .Append("t0.*, SUM(")
                            .Append(sumBy(EntityTable<T, C>.ColumnsDefine), p)
                        .Append(") OVER(PARTITION BY ")
                            .Append(partitionBy(EntityTable<T, C>.ColumnsDefine)).Append(' ')
                        .AppendOrder(_closure.Orderby)
                        .Append(" ROWS UNBOUNDED PRECEDING) AS accSum").ToString();

                    _genSql(sql, tmpSelect, _closure, p);

                    sql.Append(") t0 WHERE t0.accSum <= :").Append(p.Add(p.Count.ToString(), sum).ParameterName);

                    if (c.Filters.Any() || c.Orderby.Any())
                        sql.Insert(i + 7 + select.Length, " FROM (SELECT *").Append(") t0").AppendWhere(p, c.Filters).AppendOrder(c.Orderby);

                    sql.MappingAlias(i, c.Tables.First());
                },
                newC);
        }
        #endregion
        #region TakePage
        public QueryContext<C, T, TResult> TakeByPage(int pageNo, int pageSize)
        {
            int skip = (pageNo - 1) * pageSize;
            var newC = _closure;
            newC.Filters = Enumerable.Empty<Predicate>();
            newC.Orderby = Enumerable.Empty<SortDescription>();
            return new QueryContext<C, T, TResult>(
                _db,
                _projection,
                (sql, select, c, p) =>
                {
                    // SELECT [select]                FROM (SELECT a.* , ROWNUM AS rn FROM (sql)a )t0 WHERE t0.rn > [skip] AND ROWNUM <= [pageSize]
                    // SELECT [select] FROM (SELECT * FROM (SELECT a.* , ROWNUM AS rn FROM (sql)a )t0 WHERE t0.rn > [skip] AND ROWNUM <= [pageSize])t0 [WHERE .. ORDER BY ..]
                    sql.Append("SELECT ");
                    int i = sql.Length;

                    sql.Append(select).Append(" FROM (SELECT a.* ,ROWNUM AS rn FROM (");

                    _genSql(sql, "t0.*", _closure, p);

                    sql.Append(")a )t0 WHERE t0.rn > :").Append(p.Add(p.Count.ToString(), skip).ParameterName)
                        .Append(" AND ROWNUM <= :").Append(p.Add(p.Count.ToString(), pageSize).ParameterName);

                    if (c.Filters.Any() || c.Orderby.Any())
                        sql.Insert(i + select.Length, " FROM (SELECT *").Append(") t0").AppendWhere(p, c.Filters).AppendOrder(c.Orderby);

                    sql.MappingAlias(i, c.Tables.First());
                },
                newC);
        }
        #endregion
        #region OrderBy(Descending) / ThenBy(Descending)
        //public QueryContext<T, C, TResult> OrderBy(Func<C, IEnumerable<ColumnSortDescription>> keySelector)
        //{
        //    var newC = _closure;
        //    newC.Orderby = EnumerableEx.Concat(
        //        _closure.Orderby,
        //        from order in keySelector(EntityTable<T, C>.ColumnsDefine)
        //        where Table<T>.DbColumnMap.ContainsKey(order.ColumnName)
        //        select new SortDescription("t0." + order.ColumnName, order.Descending));
        //    return new QueryContext<T, C, TResult>(_db, _projection, _genSql, newC);
        //}
        public QueryContext<C, T, TResult> OrderBy(Func<C, IDbExpression> keySelector)
        {
            return OrderBy(keySelector(EntityTable<T, C>.ColumnsDefine));
        }
        public QueryContext<C, T, TResult> OrderByDescending(Func<C, IDbExpression> keySelector)
        {
            return OrderBy(keySelector(EntityTable<T, C>.ColumnsDefine), true);
        }

        public QueryContext<C, T, TResult> ThenBy(Func<C, IDbExpression> keySelector)
        {
            return OrderBy(keySelector(EntityTable<T, C>.ColumnsDefine));
        }
        public QueryContext<C, T, TResult> ThenByDescending(Func<C, IDbExpression> keySelector)
        {
            return OrderBy(keySelector(EntityTable<T, C>.ColumnsDefine), true);
        }

        QueryContext<C, T, TResult> OrderBy(IDbExpression expr, bool desc = false)
        {
            var newC = _closure;
            newC.Orderby = EnumerableEx.Concat(_closure.Orderby, new SortDescription(expr, desc));
            return new QueryContext<C, T, TResult>(_db, _projection, _genSql, newC);
        }
        #endregion
        #region Where
        public QueryContext<C, T, TResult> Where(Func<C, Predicate> predicate)
        {
            var filter = predicate(EntityTable<T, C>.ColumnsDefine);
            if (!filter.IsVaild)
                return this;
            var newC = _closure;
            newC.Filters = EnumerableEx.Concat(_closure.Filters, filter);
            return new QueryContext<C, T, TResult>(_db, _projection, _genSql, newC);
        }
        #endregion
        #region Select
        public QueryContext<C, T, TResult> Select(Func<C, C> selector)
        {
            return this;
        }

        public QueryContext<C, T, TR> Select<TR>(Expression<Func<T, TR>> selector, [CallerFilePath]string file = "", [CallerLineNumber]int line = 0)
        {
            return new QueryContext<C, T, TR>(_db, new Lazy<Projection>(() => Projection.Create(selector, file, line)), _genSql, _closure);
        }
        #endregion
        #region SelectMany
        public SelectManyContext<C, T, TResult, _> SelectMany<C2, T2, TResult2, _>(Func<C, QueryContext<C2, T2, TResult2>> collectionSelector, Func<C, C2, _> resultSelector) where T2 : DbEntity
        {
            var innerContext = collectionSelector(EntityTable<T, C>.ColumnsDefine);

            var newC = _closure;
            newC.Tables = EnumerableEx.Concat(_closure.Tables, innerContext);

            return new SelectManyContext<C, T, TResult, _>(
                db: _db,
                transparentId: resultSelector(EntityTable<T, C>.ColumnsDefine, EntityTable<T2, C2>.ColumnsDefine),
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
        #region Distinct
        public QueryContext<C, T, TResult> Distinct()
        {
            var newC = _closure;
            newC.Distinct = true;
            return new QueryContext<C, T, TResult>(_db, _projection, _genSql, newC);
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
            newC.Orderby = Enumerable.Empty<SortDescription>();
            return new GroupingContextCollection<C, T, TKey, TResult>(
                new QueryContext<C, T, TResult>(_db, _projection, _genSql, newC),
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
        QueryContext<C, T, TResult> SetOp(QueryContext<C, T, TResult> other, string op)
        {
            var thisC = _closure; thisC.Orderby = Enumerable.Empty<SortDescription>();
            var otherC = other._closure; otherC.Orderby = Enumerable.Empty<SortDescription>();
            return new QueryContext<C, T, TResult>(
                _db,
                _projection,
                (sql, s, c, p) =>
                {
                    int i = sql.Length;
                    sql.Append("SELECT ").Append(s).Append(" FROM (");
                    this._genSql(sql, "t0.*", thisC, p);
                    sql.Append(op);
                    other._genSql(sql, "t0.*", otherC, p);
                    sql.Append(") t0").AppendWhere(p, c.Filters).AppendOrder(c.Orderby).MappingAlias(i, c.Tables.First());
                });
        }

        public QueryContext<C, T, TResult> Intersect(QueryContext<C, T, TResult> other)
        {
            return SetOp(other, " INTERSECT ");
        }
        public QueryContext<C, T, TResult> Except(QueryContext<C, T, TResult> other)
        {
            return SetOp(other, " MINUS ");
        }
        public QueryContext<C, T, TResult> Union(QueryContext<C, T, TResult> other)
        {
            return SetOp(other, " UNION ");
        }
        public QueryContext<C, T, TResult> Concat(QueryContext<C, T, TResult> other)
        {
            return SetOp(other, " UNION ALL ");
        }
        #endregion
        #region Max / Min / Sum / Average
        #region Max / Min
        string Max(Func<C, Linq2Oracle.Expressions.String> selector)
        {
            return (string)_AggregateFunction(new DbExpression().Init((sql, param) =>
                sql.Append("MAX(").Append(selector(EntityTable<T, C>.ColumnsDefine), param).Append(')')));
        }
        public string Min(Func<C, Linq2Oracle.Expressions.String> selector)
        {
            return (string)_AggregateFunction(new DbExpression().Init((sql, param) =>
                sql.Append("MIN(").Append(selector(EntityTable<T, C>.ColumnsDefine), param).Append(')')));
        }

        TR? Max<TR>(Func<C, IDbExpression<TR>> selector) where TR : struct
        {
            return _MaxMin<TR>("MAX", selector);
        }

        public TR? Max<TR>(Func<C, INullable<TR>> selector)
            where TR : struct
        {
            return _MaxMin<TR>("MAX", selector);
        }

        public TR? Min<TR>(Func<C, IDbExpression<TR>> selector) where TR : struct
        {
            return _MaxMin<TR>("MIN", selector);
        }

        public TR? Min<TR>(Func<C, INullable<TR>> selector) where TR : struct
        {
            return _MaxMin<TR>("MIN", selector);
        }

        TR? _MaxMin<TR>(string function, Func<C, IDbExpression> selector) where TR : struct
        {
            var value = _AggregateFunction(new DbExpression().Init((sql,param)=>
                sql.Append(function).Append('(').Append(selector(EntityTable<T, C>.ColumnsDefine), param).Append(')')));
            var retType = typeof(TR);
            if (retType.IsEnum)
            {
                if (value != null)
                    return (TR?)Enum.Parse(typeof(TR), (string)value);
            }
            else
            {
                if (value != null)
                {
                    if (value is decimal)
                        return (TR?)Convert.ChangeType((decimal)value, typeof(TR));
                    return (TR?)value;
                }
            }

            return null;
        }
        #endregion
        #region Sum
        public decimal? Sum<TNumber>(Func<C, Number<TNumber>> selector) where TNumber : struct
        {
            return (decimal?)_AggregateFunction(new DbExpression().Init((sql, param) =>
                sql.Append("SUM(").Append(selector(EntityTable<T, C>.ColumnsDefine), param).Append(')')));
        }

        public decimal? Sum<TNumber>(Func<C,INullable<TNumber>> selector) where TNumber : struct
        {
            return (decimal?)_AggregateFunction(new DbExpression().Init((sql, param) =>
                sql.Append("SUM(").Append(selector(EntityTable<T, C>.ColumnsDefine), param).Append(')')));
        }
        #endregion
        #region Average
        public decimal? Average<TNumber>(Func<C, Number<TNumber>> selector) where TNumber : struct
        {
            return (decimal?)_AggregateFunction(new DbExpression().Init((sql,param)=>
                sql.Append("ROUND(AVG(").Append(selector(EntityTable<T, C>.ColumnsDefine), param).Append("),25)")));
        }

        public decimal? Average<TNumber>(Func<C, INullable<TNumber>> selector) where TNumber : struct
        {
            return (decimal?)_AggregateFunction(new DbExpression().Init((sql, param) =>
                sql.Append("ROUND(AVG(").Append(selector(EntityTable<T, C>.ColumnsDefine), param).Append("),25)")));
        }
        #endregion

        object _AggregateFunction(IDbExpression expr)
        {
            var cc = _closure;
            cc.Orderby = Enumerable.Empty<SortDescription>();
            using (var cmd = _db.CreateCommand())
            {
                var sql = new StringBuilder();
                _genSql(sql, new StringBuilder().Append(expr, cmd.Parameters).ToString(), cc, cmd.Parameters);
                cmd.CommandText = sql.ToString();
                var result = _db.ExecuteScalar(cmd);
                return result == DBNull.Value ? null : result;           
            }
        }
        #endregion
        #region Count
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        public int Count()
        {
            var cc = _closure; cc.Orderby = Enumerable.Empty<SortDescription>();
            var selection = _projection.Value.SelectSql;
            using (var cmd = _db.CreateCommand())
            {
                var sql = new StringBuilder();
                if (_closure.Distinct)
                {
                    if (selection.IndexOf(',') == -1)
                        _genSql(sql, "COUNT(DISTINCT " + selection + ")", cc, cmd.Parameters);
                    else
                    {
                        sql.Append("SELECT COUNT(*) FROM (");
                        _genSql(sql, "DISTINCT " + selection, cc, cmd.Parameters);
                        sql.Append(")");
                    }
                }
                else
                    _genSql(sql, "COUNT(*)", cc, cmd.Parameters);
                cmd.CommandText = sql.ToString();
                return (int)(decimal)_db.ExecuteScalar(cmd);
            }
        }
        public int Count(Func<C, Predicate> predicate)
        {
            return this.Where(predicate).Count();
        }
        #endregion
        #region Any
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        public bool Any()
        {
            var cc = _closure; cc.Orderby = Enumerable.Empty<SortDescription>();
            using (var cmd = _db.CreateCommand())
            {
                var sql = new StringBuilder("SELECT CASE WHEN (EXISTS(SELECT NULL FROM (");
                _genSql(sql, "*", cc, cmd.Parameters);
                sql.Append("))) THEN 1 ELSE 0 END value FROM DUAL");

                cmd.CommandText = sql.ToString();
                return (decimal)_db.ExecuteScalar(cmd) == 1;
            }
        }
        public bool Any(Func<C, Predicate> predicate)
        {
            return this.Where(predicate).Any();
        }
        #endregion
        #region All
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        public bool All(Func<C, Predicate> predicate)
        {
            var filter = predicate(EntityTable<T, C>.ColumnsDefine);
            if (!filter.IsVaild)
                throw new DalException(DbErrorCode.E_DB_SQL_INVAILD, "All條件有誤");
            var cc = _closure; cc.Orderby = Enumerable.Empty<SortDescription>();
            using (var cmd = _db.CreateCommand())
            {
                var sql = new StringBuilder("SELECT CASE WHEN (NOT EXISTS(SELECT * FROM (");
                _genSql(sql, "*", cc, cmd.Parameters);
                sql.Append(") a WHERE NOT (");
                filter.Build(sql, cmd.Parameters);
                sql.Replace(Table<T>.TableName + ".", "a.")
                    .Append("))) THEN 1 ELSE 0 END value FROM DUAL");
                cmd.CommandText = sql.ToString();
                return (decimal)_db.ExecuteScalar(cmd) == 1;
            }
        }
        #endregion
        #region IsEmpty
        public bool IsEmpty()
        {
            return !Any();
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
            return new QueryContext<C, T, TResult>(_db, _projection, _genSql, newClosure);
        }
        #endregion
        #region IQueryContext 成員

        void GenInnerSql(StringBuilder sql, OracleParameterCollection param, string selection)
        {
            var newC = _closure; newC.Orderby = Enumerable.Empty<SortDescription>();
            if (newC.Distinct)
                selection = "DISTINCT " + selection;
            _genSql(sql, selection, newC, param);
        }

        void IQueryContext.GenInnerSql(StringBuilder sql, OracleParameterCollection param)
        {
            GenInnerSql(sql, param, _projection.Value.SelectSql);
        }

        void IQueryContext.GenInnerSql(StringBuilder sql, OracleParameterCollection param, string selection)
        {
            GenInnerSql(sql, param, selection);
        }

        void IQueryContext.GenBatchSql(StringBuilder sql, OracleParameterCollection param)
        {
            var refParam = param[param.Count - 1];
            _data = EnumerableEx.Using(() => refParam, p =>
                    EnumerableEx.Using(() => ((OracleRefCursor)p.Value).GetDataReader(), reader =>
                        typeof(T) == typeof(TResult) ? (IEnumerable<TResult>)ReadIdentityEntities(reader) :
                     _projection.Value.IsProjection ? ReadProjectionResult(reader, (Func<OracleDataReader, TResult>)_projection.Value.Projector) :
                     ReadConvertedEntities(reader, (Func<T, TResult>)_projection.Value.Projector)));
            string select = _projection.Value.SelectSql;
            if (_closure.Distinct)
                select = "DISTINCT " + select;
            _genSql(sql, select, _closure, param);
            sql.AppendForUpdate<T, TResult>(_closure.ForUpdate);
        }

        public OracleDB Db { get { return _db; } }

        public string TableName { get { return Table<T>.TableName; } }
        #endregion
    }

    public sealed class EntityTable<T, C> : QueryContext<C, T, T> where T : DbEntity
    {
        internal static readonly C ColumnsDefine;
        static EntityTable()
        {
            var type = typeof(C);
            ColumnsDefine = (C)Activator.CreateInstance(type);
            foreach (var prop in type.GetProperties())
            {
                DbColumn c;
                if (!Table<T>.DbColumnMap.TryGetValue(prop.Name, out c) || !typeof(DbExpression).IsAssignableFrom(prop.PropertyType))
                    continue;
                var value = (IDbExpression)Activator.CreateInstance(prop.PropertyType);
                //value.SetColumnInfo(c.TableQuotesColumnName, c);
                prop.SetValue(ColumnsDefine, value, null);
            }
        }

        static readonly Lazy<Projection> identityProjection = new Lazy<Projection>(() => Projection.Identity<T>());

        public EntityTable(OracleDB db) : base(db, identityProjection) { }

        #region Delete
        /// <summary>
        /// 刪除多筆紀錄，用於刪除關聯紀錄
        /// </summary>
        /// <param name="predicate">刪除條件</param>
        /// <returns>實際刪除筆數</returns>
        public int Delete(Func<C, Predicate> predicate)
        {
            var filter = predicate(ColumnsDefine);
            if (!filter.IsVaild)
                return 0;

            using (var cmd = _db.CreateCommand())
            {
                var sql = new StringBuilder(32);
                sql.Append("DELETE FROM ").Append(Table<T>.TableName).Append(" WHERE ");
                filter.Build(sql, cmd.Parameters);
                cmd.CommandText = sql.ToString();
                return _db.ExecuteNonQuery(cmd);
            }
        }
        #endregion
    }

    struct Closure
    {
        public IEnumerable<IQueryContext> Tables;
        public IEnumerable<Predicate> Filters;
        public IEnumerable<SortDescription> Orderby;
        public bool Distinct;

        //public bool IncludeTotalCount;
        //public Action<int> IncludeCount;
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

    public struct ColumnSortDescription
    {
        public string ColumnName { get; set; }
        public bool Descending { get; set; }
    }
}
