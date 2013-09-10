using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using Linq2Oracle.Expressions;
using System;

namespace Linq2Oracle
{
    using SqlGenerator = Action<SqlContext, string, Closure>;

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
        readonly IReadOnlyList<Boolean> _having;

        internal GroupingContextCollection(QueryContext<C, T, TElement> context, Expression<Func<T, TKey>> keySelector)
        {
            this._context = context;
            this._keySelector = new System.Lazy<GroupingKeySelector>(() => GroupingKeySelector.Create(keySelector));
            this._having = EmptyList<Boolean>.Instance;
        }

        GroupingContextCollection(QueryContext<C, T, TElement> context, Lazy<GroupingKeySelector> keySelector, IReadOnlyList<Boolean> filters)
        {
            this._context = context;
            this._keySelector = keySelector;
            this._having = filters;
        }

        /// <summary>
        /// Debug infomation
        /// </summary>
        IEnumerable<TKey> Keys { get { return this.Select(g => g.Key); } }

        #region Where(Having)
        /// <summary>
        /// SQL Having operator
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public GroupingContextCollection<C, T, TKey, TElement> Where(Func<HavingContext<T, C>, Boolean> predicate)
        {
            return new GroupingContextCollection<C, T, TKey, TElement>(
                context: _context,
                keySelector: _keySelector,
                filters: new List<Boolean>(_having) { predicate(new HavingContext<T, C>(_context.ColumnDefine)) });
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
        public IQueryContext<TResult> Select<TResult>(Expression<Func<IGroupingAggregateContext<T, TKey>, TResult>> selector, [CallerFilePath]string file = "", [CallerLineNumber]int line = 0)
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
                    newClosure.Filters = new List<Boolean>(_context._closure.Filters) { keyPredicate };

                yield return new GroupingContext<C, T, TKey, TElement>(_context.OriginalSource, key, _context.Db, _context._projection, _context._genSql, newClosure, _context.ColumnDefine);
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

        internal GroupingContext(IQueryContext originalSource, TKey key, OracleDB db, Lazy<Projection> projector, SqlGenerator genSql, Closure closure, C columnDefine)
            : base(db, projector, closure, originalSource, genSql, columnDefine)
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
        public Number<int> Count()
        {
            return new Number<int>().Init(OracleDbType.Int32,
                sql => sql.Append("COUNT(*)"));
        }

        public Number<long> LongCount()
        {
            return new Number<long>().Init(OracleDbType.Int64,
                sql => sql.Append("COUNT(*)"));
        }
        #endregion
        #region Average
        public Number<decimal> Average<N>(Func<C, Number<N>> selector) where N : struct
        {
            return new Number<decimal>().Init(OracleDbType.Decimal, sql =>
                sql.Append("AVG(").Append(selector(ColumnDefine)).Append(')'));
        }

        public NullableNumber<decimal> Average<N>(Func<C, NullableNumber<N>> selector) where N : struct
        {
            return new NullableNumber<decimal>().Init(OracleDbType.Decimal, sql =>
                sql.Append("AVG(").Append(selector(ColumnDefine)).Append(')'));
        }
        #endregion
        #region Sum
        public Number<N> Sum<N>(Func<C, Number<N>> selector) where N : struct
        {
            return new Number<N>().Init(OracleDbType.Decimal, sql =>
                sql.Append("SUM(").Append(selector(ColumnDefine)).Append(')'));
        }

        public NullableNumber<N> Sum<N>(Func<C, NullableNumber<N>> selector) where N : struct
        {
            return new NullableNumber<N>().Init(OracleDbType.Decimal, sql =>
                sql.Append("SUM(").Append(selector(ColumnDefine)).Append(')'));
        }
        #endregion
        #region Max / Min
        public TColumn Max<TColumn>(Func<C, TColumn> selector) where TColumn : IDbExpression, new()
        {
            var c = selector(ColumnDefine);
            var result = new TColumn();
            ((ISqlExpressionBuilder)result).Init(c.DbType,
                sql => sql.Append("MAX(").Append(selector(ColumnDefine)).Append(')'));
            return result;
        }

        public TColumn Min<TColumn>(Func<C, TColumn> selector) where TColumn : IDbExpression, new()
        {
            var c = selector(ColumnDefine);
            var result = new TColumn();
            ((ISqlExpressionBuilder)result).Init(c.DbType, sql =>
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

        readonly IEnumerable<Boolean> _having;

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        readonly Lazy<GroupingAggregate> _aggregate;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        internal AggregateResult(Lazy<GroupingAggregate> aggregate, IQueryContext context, IEnumerable<Boolean> having)
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
