using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace Linq2Oracle {
    using SqlGenerator = Action<StringBuilder, string, Closure, OracleParameterCollection>;
    using System.Runtime.CompilerServices;

    /// <summary>
    /// GroupBy查詢結果
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TElement"></typeparam>
    public sealed class GroupingContextCollection<T, C, TKey, TElement> : IQueryContext<GroupingContext<T, C, TKey, TElement>> where T : DbEntity
    {
        readonly QueryContext<T, C, TElement> _context;
        readonly QueryContext<T, C, TKey> _keyContext;
        readonly Lazy<GroupingKeySelector> _keySelector;


        internal GroupingContextCollection(QueryContext<T, C, TElement> context, Expression<Func<T, TKey>> keySelector)
        {
            this._context = context;
            this._keyContext = context.Select(keySelector).Distinct();
            this._keySelector = new Lazy<GroupingKeySelector>(() => GroupingKeySelector.Create(keySelector));
        }

        public IQueryContext<TResult> Select<TResult>(Expression<Func<IGroupingContext<T, TKey>, TResult>> selector, [CallerFilePath]string file = "", [CallerLineNumber]int line = 0)
        {
            return new AggregateResult<TResult>(new Lazy<GroupingAggregate>(() => GroupingAggregate.Create(_keySelector.Value, selector)), _context);
        }
        #region IQueryContext 成員
        void IQueryContext.GenInnerSql(StringBuilder sql, OracleParameterCollection param)
        {
            ((IQueryContext)_keyContext).GenInnerSql(sql, param);
        }

        void IQueryContext.GenInnerSql(StringBuilder sql, OracleParameterCollection param, string selection)
        {
            ((IQueryContext)_keyContext).GenInnerSql(sql, param, selection);
        }

        void IQueryContext.GenBatchSql(StringBuilder sql, OracleParameterCollection param)
        {
            ((IQueryContext)_keyContext).GenBatchSql(sql, param);
        }

        public OracleDB Db { get { return _context.Db; } }

        public string TableName { get { return Table<T>.TableName; } }
        #endregion
        #region IEnumerable<GroupingContext<T,Q,S,TKey,TElement>> 成員
        public IEnumerator<GroupingContext<T, C, TKey, TElement>> GetEnumerator()
        {
            foreach (var key in _keyContext)
            {
                var newClosure = _context._closure;
                newClosure.Filters = EnumerableEx.Concat(_context._closure.Filters, EnumerableEx.Return(_keySelector.Value.GetGroupKeyPredicate(key)));
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

    public interface IGroupingContext<T, TKey> where T : DbEntity
    {
        TKey Key { get; }
        double Average(Expression<Func<T, long>> selector);
        double Average(Expression<Func<T, float>> selector);
        TR Max<TR>(Expression<Func<T, TR>> selector);
        TR Min<TR>(Expression<Func<T, TR>> selector);
        long Sum(Expression<Func<T, long>> selector);
        long Count();
    }

    [DebuggerDisplay("查詢 {TableName}")]
    sealed class AggregateResult<T> : IQueryContext<T>
    {
        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        IEnumerable<T> _data;

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        readonly IQueryContext _context;

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        readonly Lazy<GroupingAggregate> _aggregate;
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        internal AggregateResult(Lazy<GroupingAggregate> aggregate, IQueryContext context)
        {
            _context = context;
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

        [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
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
                    SQL_PARAM = param.Cast<OracleParameter>().Select(p => p.Value).ToArray()
                };
            }
        }

        #region IQueryContext 成員
        void GenSql(StringBuilder sql, OracleParameterCollection param)
        {
            int i = sql.Length;
            sql.Append("SELECT ").Append(_aggregate.Value.SelectionSql).Append(" FROM (");
            _context.GenInnerSql(sql, param, "t0.*");
            sql.Append(") t0 GROUP BY ").Append(_aggregate.Value.GrouipingKeySelector.GroupKeySql)
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
