using Linq2Oracle.Expressions;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;

namespace Linq2Oracle
{
    using SqlGenerator = System.Action<SqlContext, string, Closure>;

    /// <summary>
    /// GroupBy查詢結果
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="C"></typeparam>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TElement"></typeparam>
    public sealed class GroupingContextCollection<C, T, TKey, TElement> : IQueryContext<GroupingContext<C, T, TKey, TElement>>
        where T : DbEntity
        where C : class,new()
    {
        readonly QueryContext<C, T, TElement> _context;
        readonly System.Lazy<GroupingKeySelector> _keySelector;
        readonly IReadOnlyList<SqlBoolean> _having;

        internal GroupingContextCollection(QueryContext<C, T, TElement> context, Expression<System.Func<T, TKey>> keySelector)
        {
            this._context = context;
            this._keySelector = new System.Lazy<GroupingKeySelector>(() => GroupingKeySelector.Create(keySelector));
            this._having = EmptyList<SqlBoolean>.Instance;
        }

        GroupingContextCollection(QueryContext<C, T, TElement> context, System.Lazy<GroupingKeySelector> keySelector, IReadOnlyList<SqlBoolean> filters)
        {
            this._context = context;
            this._keySelector = keySelector;
            this._having = filters;
        }

        /// <summary>
        /// Debug infomation
        /// </summary>
        IEnumerable<TKey> Keys { get { return this.Select(g => g.Key).AsEnumerable(); } }

        #region Where(Having)
        /// <summary>
        /// SQL Having operator
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public GroupingContextCollection<C, T, TKey, TElement> Where(System.Func<HavingContext<T, C>, SqlBoolean> predicate)
        {
            return new GroupingContextCollection<C, T, TKey, TElement>(
                context: _context,
                keySelector: _keySelector,
                filters: new List<SqlBoolean>(_having) { predicate(new HavingContext<T, C>(_context.ColumnDefine)) });
        }
        #endregion
        #region Select
        /// <summary>
        /// SQL Group Aggregation Projection
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="selector"></param>
        /// <param name="file"></param>
        /// <param name="line"></param>
        /// <returns></returns>
        public IQueryContext<TResult> Select<TResult>(Expression<System.Func<IGroupingAggregateContext<T, TKey>, TResult>> selector, [CallerFilePath]string file = "", [CallerLineNumber]int line = 0)
        {
            return new AggregateResult<TResult>(new System.Lazy<GroupingAggregate>(() => GroupingAggregate.Create(_keySelector.Value, selector)), _context, _having);
        }
        #endregion
        #region IQueryContext 成員
        void IQueryContext.GenInnerSql(SqlContext sql, string selection)
        {
            ((IQueryContext)this.Select(g => g.Key)).GenInnerSql(sql, selection);
        }

        void IQueryContext.GenBatchSql(SqlContext sql, OracleParameter refParam)
        {
            ((IQueryContext)this.Select(g => g.Key)).GenBatchSql(sql, refParam);
        }

        public OracleDB Db { get { return _context.Db; } }

        public string TableName { get { return Table<T>.TableName; } }

        IQueryContext IQueryContext.OriginalSource
        {
            get { return _context.OriginalSource; }
        }
        #endregion
        #region IEnumerator<GroupingContext<C, T, TKey, TElement>> 成員
        public IEnumerator<GroupingContext<C, T, TKey, TElement>> GetEnumerator()
        {
            foreach (var key in this.Select(g => g.Key))
            {
                var keyPredicate = _keySelector.Value.GetGroupKeyPredicate(key);
                var newClosure = _context._closure;
                if (keyPredicate.IsVaild)
                    newClosure.Filters = new List<SqlBoolean>(_context._closure.Filters) { keyPredicate };

                yield return new GroupingContext<C, T, TKey, TElement>(_context, key, newClosure);
            }
        }
        #endregion
        #region IEnumerable 成員
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
        #endregion
    }

    [DebuggerDisplay("{Key}")]
    public sealed class GroupingContext<C, T, TKey, TElement> : QueryContext<C, T, TElement>
        where T : DbEntity
        where C : class,new()
    {
        public TKey Key { get; private set; }

        internal GroupingContext(QueryContext<C, T, TElement> context, TKey key, Closure closure)
            : base(context._projection, closure, context._genSql, context.ColumnDefine)
        {
            Key = key;
        }
    }

    /// <summary>
    /// SQL Having Clause Context
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="C"></typeparam>
    public sealed class HavingContext<T, C> where T : DbEntity
    {
        readonly C ColumnDefine;
        internal HavingContext(C columnDefine)
        {
            this.ColumnDefine = columnDefine;
        }
        #region Count / LongCount
        public DbNumber Count()
        {
            return new DbNumber().Init(sql => sql.Append("COUNT(*)"));
        }

        public DbNumber LongCount()
        {
            return new DbNumber().Init(sql => sql.Append("COUNT(*)"));
        }
        #endregion
        #region Average
        public DbNumber Average(System.Func<C, DbNumber> selector)
        {
            return new DbNumber().Init(Function.Call("AVG", selector(ColumnDefine)));
        }

        public NullableDbNumber Average(System.Func<C, NullableDbNumber> selector)
        {
            return new NullableDbNumber().Init(Function.Call("AVG", selector(ColumnDefine)));
        }
        #endregion
        #region Sum
        public DbNumber Sum(System.Func<C, DbNumber> selector) 
        {
            var c = selector(ColumnDefine);
            return new DbNumber().Init(Function.Call("SUM", selector(ColumnDefine)));
        }

        public NullableDbNumber Sum(System.Func<C, NullableDbNumber> selector)
        {
            var c = selector(ColumnDefine);
            return new NullableDbNumber().Init(Function.Call("SUM", selector(ColumnDefine)));
        }
        #endregion
        #region Max / Min
        public TColumn Max<TColumn>(System.Func<C, TColumn> selector) where TColumn : IDbExpression, new()
        {
            var c = selector(ColumnDefine);
            var result = new TColumn().Init(sql => sql.Append("MAX(").Append(selector(ColumnDefine)).Append(')'));
            return result;
        }

        public TColumn Min<TColumn>(System.Func<C, TColumn> selector) where TColumn : IDbExpression, new()
        {
            var c = selector(ColumnDefine);
            var result = new TColumn().Init(sql =>
                sql.Append("MIN(").Append(selector(ColumnDefine)).Append(')'));
            return result;
        }
        #endregion
    }

    /// <summary>
    /// SQL Aggregation Group 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TKey"></typeparam>
    public interface IGroupingAggregateContext<T, TKey> where T : DbEntity
    {
        TKey Key { get; }

        #region Count / Long Count
        int Count();
        long LongCount();
        #endregion
        #region Average
        double Average(Expression<System.Func<T, short>> selector);
        double Average(Expression<System.Func<T, int>> selector);
        double Average(Expression<System.Func<T, long>> selector);
        float Average(Expression<System.Func<T, float>> selector);
        double Average(Expression<System.Func<T, double>> selector);
        decimal Average(Expression<System.Func<T, decimal>> selector);

        double? Average(Expression<System.Func<T, short?>> selector);
        double? Average(Expression<System.Func<T, int?>> selector);
        double? Average(Expression<System.Func<T, long?>> selector);
        float? Average(Expression<System.Func<T, float?>> selector);
        double? Average(Expression<System.Func<T, double?>> selector);
        decimal? Average(Expression<System.Func<T, decimal?>> selector);
        #endregion
        #region Sum
        int Sum(Expression<System.Func<T, int>> selector);
        long Sum(Expression<System.Func<T, long>> selector);
        float Sum(Expression<System.Func<T, float>> selector);
        double Sum(Expression<System.Func<T, double>> selector);
        decimal Sum(Expression<System.Func<T, decimal>> selector);

        int? Sum(Expression<System.Func<T, int?>> selector);
        long? Sum(Expression<System.Func<T, long?>> selector);
        float? Sum(Expression<System.Func<T, float?>> selector);
        double? Sum(Expression<System.Func<T, double?>> selector);
        decimal? Sum(Expression<System.Func<T, decimal?>> selector);
        #endregion
        #region Max / Min
        TR Max<TR>(Expression<System.Func<T, TR>> selector);
        TR Min<TR>(Expression<System.Func<T, TR>> selector);
        #endregion
    }

    [DebuggerDisplay("查詢 {TableName}")]
    sealed class AggregateResult<T> : IQueryContext<T>
    {

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        IEnumerable<T> _data;

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        readonly IQueryContext _context;

        readonly IEnumerable<SqlBoolean> _having;

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        readonly System.Lazy<GroupingAggregate> _aggregate;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        internal AggregateResult(System.Lazy<GroupingAggregate> aggregate, IQueryContext context, IEnumerable<SqlBoolean> having)
        {
            _context = context;
            _having = having;
            _aggregate = aggregate;
            _data = EnumerableEx.Using(() => _context.Db.CreateCommand(), cmd =>
            {
                var sql = new SqlContext(new StringBuilder(), cmd.Parameters);
                GenSql(sql);
                cmd.CommandText = sql.ToString();
                return EnumerableEx.Using(() => _context.Db.ExecuteReader(cmd), reader =>
                      ReadProjectionResult(reader));
            });
        }

        IEnumerable<T> ReadProjectionResult(OracleDataReader reader)
        {
            var projector = (System.Func<OracleDataReader, T>)_aggregate.Value.ValueSelector;
            while (reader.Read())
                yield return projector(reader);
        }

        object DebugInfo
        {
            get
            {
                var param = new OracleCommand().Parameters;
                var sql = new SqlContext(new StringBuilder(), param);
                GenSql(sql);
                return new
                {
                    SQL = sql.ToString(),
                    SQL_PARAM = param.Cast<OracleParameter>().Select(p => p.Value).ToArray(),
                };
            }
        }

        #region IQueryContext 成員
        void GenSql(SqlContext sql)
        {
            sql.Append("SELECT ").Append(_aggregate.Value.SelectionSql).Append(" FROM (")
                .Append("SELECT " + sql.GetAlias(_context) + ".*", _context)
                .Append(") ").Append(sql.GetAlias(_context))
                .Append(" GROUP BY ").Append(_aggregate.Value.GrouipingKeySelector.GroupKeySql).MappingAlias(_context)
                .AppendHaving(_having);
        }

        void IQueryContext.GenInnerSql(SqlContext sql, string selection)
        {
            GenSql(sql);
        }

        void IQueryContext.GenBatchSql(SqlContext sql, OracleParameter refParam)
        {
            _data = EnumerableEx.Using(() => refParam, p =>
                    EnumerableEx.Using(() => ((OracleRefCursor)p.Value).GetDataReader(), reader =>
                            ReadProjectionResult(reader)));
            GenSql(sql);
        }

        OracleDB IQueryContext.Db { get { return _context.Db; } }

        public string TableName { get { return _context.TableName; } }

        IQueryContext IQueryContext.OriginalSource
        {
            get { return _context.OriginalSource; }
        }
        #endregion
        #region IEnumerable<T> 成員
        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return _data.GetEnumerator();
        }
        #endregion
        #region IEnumerable 成員
        IEnumerator IEnumerable.GetEnumerator()
        {
            return _data.GetEnumerator();
        }
        #endregion
    }
}
