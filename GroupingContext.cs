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
    using SqlGenerator = Action<StringBuilder, string, Closure, OracleParameterCollection>;

    /// <summary>
    /// GroupBy查詢結果
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="C"></typeparam>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TElement"></typeparam>
    public sealed class GroupingContextCollection<T, C, TKey, TElement> : IQueryContext<GroupingContext<T, C, TKey, TElement>> where T : DbEntity
    {
        readonly QueryContext<T, C, TElement> _context;
        readonly Lazy<GroupingKeySelector> _keySelector;
        readonly IReadOnlyList<Predicate> _having;

        internal GroupingContextCollection(QueryContext<T, C, TElement> context, Expression<Func<T, TKey>> keySelector)
        {
            this._context = context;
            this._keySelector = new Lazy<GroupingKeySelector>(() => GroupingKeySelector.Create(keySelector));
            this._having = EmptyList<Predicate>.Instance;
        }

        GroupingContextCollection(QueryContext<T, C, TElement> context, Lazy<GroupingKeySelector> keySelector, IReadOnlyList<Predicate> filters)
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
        public GroupingContextCollection<T, C, TKey, TElement> Where(Func<HavingContext<T, C>, Predicate> predicate)
        {
            return new GroupingContextCollection<T, C, TKey, TElement>(
                context: _context,
                keySelector: _keySelector,
                filters: new List<Predicate>(_having) { predicate(HavingContext<T, C>.Instance) });
        }
        #endregion

        #region Where(local filter)
        /// <summary>
        /// LINQ to Object Where operator
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public IEnumerable<GroupingContext<T, C, TKey, TElement>> Where(Func<GroupingContext<T, C, TKey, TElement>, bool> predicate)
        {
            return this.AsEnumerable().Where(predicate);
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
            return new AggregateResult<TResult>(new Lazy<GroupingAggregate>(() => GroupingAggregate.Create(_keySelector.Value, selector)), _context, _having);
        }
        #endregion
        #region IQueryContext 成員
        void IQueryContext.GenInnerSql(StringBuilder sql, OracleParameterCollection param)
        {
            ((IQueryContext)this.Select(g => g.Key)).GenInnerSql(sql, param);
        }

        void IQueryContext.GenInnerSql(StringBuilder sql, OracleParameterCollection param, string selection)
        {
            ((IQueryContext)this.Select(g => g.Key)).GenInnerSql(sql, param, selection);
        }

        void IQueryContext.GenBatchSql(StringBuilder sql, OracleParameterCollection param)
        {
            ((IQueryContext)this.Select(g => g.Key)).GenBatchSql(sql, param);
        }

        public OracleDB Db { get { return _context.Db; } }

        public string TableName { get { return Table<T>.TableName; } }
        #endregion
        #region IEnumerable<GroupingContext<T,Q,S,TKey,TElement>> 成員
        public IEnumerator<GroupingContext<T, C, TKey, TElement>> GetEnumerator()
        {
            foreach (var key in this.Select(g => g.Key))
            {
                var keyPredicate = _keySelector.Value.GetGroupKeyPredicate(key);
                var newClosure = _context._closure;
                if (keyPredicate.IsVaild)
                    newClosure.Filters = new List<Predicate>(_context._closure.Filters) { keyPredicate };

                yield return new GroupingContext<T, C, TKey, TElement>(key, _context.Db, _context._projection, _context._genSql, newClosure);
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
    public sealed class GroupingContext<T, C, TKey, TElement> : QueryContext<T, C, TElement> where T : DbEntity
    {
        public TKey Key { get; private set; }

        internal GroupingContext(TKey key, OracleDB db, Lazy<Projection> projector, SqlGenerator genSql, Closure closure)
            : base(db, projector, genSql, closure)
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
        static HavingContext() { }
        internal static readonly HavingContext<T, C> Instance = new HavingContext<T, C>();

        #region Count / LongCount
        public Number<int> Count()
        {
            return new Number<int>().Init("COUNT(*)", new DbExpressionMetaInfo { DbType = OracleDbType.Int32 });
        }

        public Number<long> LongCount()
        {
            return new Number<long>().Init("COUNT(*)", new DbExpressionMetaInfo { DbType = OracleDbType.Int64 });
        }
        #endregion
        #region Average
        public Number<decimal> Average<N>(Func<C, Number<N>> selector) where N : struct
        {
            var c = selector(EntityTable<T, C>.ColumnsDefine);
            return new Number<decimal>().Init("AVG(" + c.Expression + ")", new DbExpressionMetaInfo { DbType = OracleDbType.Decimal });
        }

        public Number<decimal?> Average<N>(Func<C, Number<N?>> selector) where N : struct
        {
            var c = selector(EntityTable<T, C>.ColumnsDefine);
            return new Number<decimal?>().Init("AVG(" + c.Expression + ")", new DbExpressionMetaInfo { DbType = OracleDbType.Decimal });
        }
        #endregion
        #region Sum
        public Number<N> Sum<N>(Func<C, Number<N>> selector) where N : struct
        {
            var c = selector(EntityTable<T, C>.ColumnsDefine);
            return new Number<N>().Init("SUM(" + c.Expression + ")", c.MetaInfo);
        }

        public Number<N?> Sum<N>(Func<C, Number<N?>> selector) where N : struct
        {
            var c = selector(EntityTable<T, C>.ColumnsDefine);
            return new Number<N?>().Init("SUM(" + c.Expression + ")", c.MetaInfo);
        }
        #endregion
        #region Max / Min
        public TColumn Max<TColumn>(Func<C, TColumn> selector) where TColumn : DbExpression, new()
        {
            var c = selector(EntityTable<T, C>.ColumnsDefine);
            return new TColumn().Init("MAX(" + c.Expression + ")", c.MetaInfo);
        }

        public TColumn Min<TColumn>(Func<C, TColumn> selector) where TColumn : DbExpression, new()
        {
            var c = selector(EntityTable<T, C>.ColumnsDefine);
            return new TColumn().Init("MIN(" + c.Expression + ")", c.MetaInfo);
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

        readonly IEnumerable<Predicate> _having;

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        readonly Lazy<GroupingAggregate> _aggregate;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        internal AggregateResult(Lazy<GroupingAggregate> aggregate, IQueryContext context, IEnumerable<Predicate> having)
        {
            _context = context;
            _having = having;
            _aggregate = aggregate;
            _data = EnumerableEx.Using(() => _context.Db.CreateCommand(), cmd =>
            {
                var sql = new StringBuilder();
                GenSql(sql, cmd.Parameters);
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
                var sql = new StringBuilder();
                GenSql(sql, param);
                return new
                {
                    SQL = sql.ToString(),
                    SQL_PARAM = param.Cast<OracleParameter>().Select(p => p.Value).ToArray(),
                };
            }
        }

        #region IQueryContext 成員
        void GenSql(StringBuilder sql, OracleParameterCollection param)
        {
            int i = sql.Length;
            sql.Append("SELECT ").Append(_aggregate.Value.SelectionSql).Append(" FROM (");
            _context.GenInnerSql(sql, param, "t0.*");
            sql.Append(") t0 GROUP BY ")
                .Append(_aggregate.Value.GrouipingKeySelector.GroupKeySql)
                .AppendHaving(param, _having)
                .MappingAlias(i, _context);
        }

        void IQueryContext.GenInnerSql(StringBuilder sql, OracleParameterCollection param)
        {
            GenSql(sql, param);
        }

        void IQueryContext.GenInnerSql(StringBuilder sql, OracleParameterCollection param, string selection)
        {
            throw new NotSupportedException("不支援此查詢方法");
        }

        void IQueryContext.GenBatchSql(StringBuilder sql, OracleParameterCollection param)
        {
            var refParam = param[param.Count - 1];
            _data = EnumerableEx.Using(() => refParam, p =>
                    EnumerableEx.Using(() => ((OracleRefCursor)p.Value).GetDataReader(), reader =>
                            ReadProjectionResult(reader)));
            GenSql(sql, param);
        }

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        OracleDB IQueryContext.Db { get { return _context.Db; } }

        public string TableName { get { return _context.TableName; } }
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
