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
using System;
using static Linq2Oracle.Expressions.Function;

namespace Linq2Oracle
{

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
        readonly Lazy<GroupingKeySelector> _keySelector;
        readonly IReadOnlyList<SqlBoolean> _having;

        internal GroupingContextCollection(QueryContext<C, T, TElement> context, Expression<Func<T, TKey>> keySelector)
        {
            _context = context;
            _keySelector = new Lazy<GroupingKeySelector>(() => GroupingKeySelector.Create(keySelector));
            _having = EmptyList<SqlBoolean>.Instance;
        }

        GroupingContextCollection(QueryContext<C, T, TElement> context, Lazy<GroupingKeySelector> keySelector, IReadOnlyList<SqlBoolean> filters)
        {
            _context = context;
            _keySelector = keySelector;
            _having = filters;
        }

        /// <summary>
        /// Debug infomation
        /// </summary>
        IEnumerable<TKey> Keys => Select(g => g.Key).AsEnumerable();

        /// <summary>
        /// SQL Having operator
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public GroupingContextCollection<C, T, TKey, TElement> Where(Func<HavingContext<T, C>, SqlBoolean> predicate)
            => new GroupingContextCollection<C, T, TKey, TElement>(
                context: _context,
                keySelector: _keySelector,
                filters: new List<SqlBoolean>(_having) { predicate(new HavingContext<T, C>(_context.ColumnDefine)) });

        /// <summary>
        /// SQL Group Aggregation Projection
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="selector"></param>
        /// <param name="file"></param>
        /// <param name="line"></param>
        /// <returns></returns>
        public IQueryContext<TResult> Select<TResult>(Expression<Func<IGroupingAggregateContext<T, TKey>, TResult>> selector, [CallerFilePath]string file = "", [CallerLineNumber]int line = 0) 
            => new AggregateResult<TResult>(new Lazy<GroupingAggregate>(() 
                => GroupingAggregate.Create(_keySelector.Value, selector)), _context, _having);

        void IQueryContext.GenInnerSql(SqlContext sql, string selection)
        {
            Select(g => g.Key).GenInnerSql(sql, selection);
        }

        void IQueryContext.GenBatchSql(SqlContext sql, OracleParameter refParam)
        {
            Select(g => g.Key).GenBatchSql(sql, refParam);
        }

        public OracleDB Db => _context.Db;

        public string TableName => Table<T>.TableName;

        IQueryContext IQueryContext.OriginalSource => _context.OriginalSource;

        public IEnumerator<GroupingContext<C, T, TKey, TElement>> GetEnumerator()
        {
            foreach (var key in Select(g => g.Key))
            {
                var keyPredicate = _keySelector.Value.GetGroupKeyPredicate(key);
                var newClosure = _context._closure;
                if (keyPredicate.IsVaild)
                    newClosure.Filters = new List<SqlBoolean>(_context._closure.Filters) { keyPredicate };

                yield return new GroupingContext<C, T, TKey, TElement>(_context, key, newClosure);
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    [DebuggerDisplay("{Key}")]
    public sealed class GroupingContext<C, T, TKey, TElement> : QueryContext<C, T, TElement>
        where T : DbEntity
        where C : class,new()
    {
        public TKey Key { get; }

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
            ColumnDefine = columnDefine;
        }

        public DbNumber Count() 
            => Function.Count().Create<DbNumber>();

        public DbNumber LongCount() 
            => Function.Count().Create<DbNumber>();

        public DbNumber Average(Func<C, DbNumber> selector) 
            => Call("AVG", selector(ColumnDefine)).Create<DbNumber>();

        public NullableDbNumber Average(Func<C, NullableDbNumber> selector) 
            => Call("AVG", selector(ColumnDefine)).Create<NullableDbNumber>();

        public DbNumber Sum(Func<C, DbNumber> selector) 
            => Call("SUM", selector(ColumnDefine)).Create<DbNumber>();

        public NullableDbNumber Sum(Func<C, NullableDbNumber> selector) 
            => Call("SUM", selector(ColumnDefine)).Create<NullableDbNumber>();

        public TColumn Max<TColumn>(Func<C, TColumn> selector) where TColumn : struct, IDbExpression 
            => Call("MAX", selector(ColumnDefine)).Create<TColumn>();

        public TColumn Min<TColumn>(Func<C, TColumn> selector) where TColumn : struct, IDbExpression 
            => Call("MIN", selector(ColumnDefine)).Create<TColumn>();
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
        double Average(Expression<Func<T, short>> selector);
        double Average(Expression<Func<T, int>> selector);
        double Average(Expression<Func<T, long>> selector);
        float Average(Expression<Func<T, float>> selector);
        double Average(Expression<Func<T, double>> selector);
        decimal Average(Expression<Func<T, decimal>> selector);

        double? Average(Expression<Func<T, short?>> selector);
        double? Average(Expression<Func<T, int?>> selector);
        double? Average(Expression<Func<T, long?>> selector);
        float? Average(Expression<Func<T, float?>> selector);
        double? Average(Expression<Func<T, double?>> selector);
        decimal? Average(Expression<Func<T, decimal?>> selector);
        #endregion
        #region Sum
        int Sum(Expression<Func<T, int>> selector);
        long Sum(Expression<Func<T, long>> selector);
        float Sum(Expression<Func<T, float>> selector);
        double Sum(Expression<Func<T, double>> selector);
        decimal Sum(Expression<Func<T, decimal>> selector);

        int? Sum(Expression<Func<T, int?>> selector);
        long? Sum(Expression<Func<T, long?>> selector);
        float? Sum(Expression<Func<T, float?>> selector);
        double? Sum(Expression<Func<T, double?>> selector);
        decimal? Sum(Expression<Func<T, decimal?>> selector);
        #endregion
        #region Max / Min
        TR Max<TR>(Expression<Func<T, TR>> selector);
        TR Min<TR>(Expression<Func<T, TR>> selector);
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
        readonly Lazy<GroupingAggregate> _aggregate;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        internal AggregateResult(Lazy<GroupingAggregate> aggregate, IQueryContext context, IEnumerable<SqlBoolean> having)
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
            var projector = (Func<OracleDataReader, T>)_aggregate.Value.ValueSelector;
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

        void GenSql(SqlContext sql)
        {
            sql.Append("SELECT ").Append(_aggregate.Value.SelectionSql).Append(" FROM (")
                .Append(sql.GetAlias(_context) + ".*", _context)
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

        OracleDB IQueryContext.Db => _context.Db;

        public string TableName => _context.TableName;

        IQueryContext IQueryContext.OriginalSource => _context.OriginalSource;

        IEnumerator<T> IEnumerable<T>.GetEnumerator() => _data.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _data.GetEnumerator();
    }
}
