using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace Linq2Oracle
{
    sealed class Projection : System.Linq.Expressions.ExpressionVisitor
    {
        static Projection() { }
        static readonly ParameterExpression _DbReader = Expression.Parameter(typeof(OracleDataReader), "reader");
#if  LOCATIONAL_CACHE
        static readonly ConcurrentDictionary<LocationalIndex, Projection> _Cache = new ConcurrentDictionary<LocationalIndex, Projection>();

        internal static Projection Create<T, TResult>(Expression<Func<T, TResult>> selector, string file, int line) where T : DbEntity
        {
            return _Cache.GetOrAdd(new LocationalIndex(file, line), _ => new Projection(Table<T>.Info, selector));
        }
#elif NO_CACHE
        internal static Projection Create<T, TResult>(Expression<Func<T, TResult>> selector, string file, int line) where T : DbEntity
        {
            return new Projection(Table<T>.Info, selector);
        }
#else
        static readonly ExpressionCache<Projection> _Cache = new ExpressionCache<Projection>();

        internal static Projection Create<T, TResult>(Expression<Func<T, TResult>> selector, string file, int line) where T : DbEntity
        {
            return _Cache.Get(selector, key => new Projection(Table<T>.Info, key));
        }
#endif
        public readonly string SelectSql;
        public readonly Delegate Projector;
        public bool IsProjection { get; private set; }
        readonly ParameterExpression _paramT;
        readonly Table.Info _tableInfo;
        readonly Dictionary<string, Expression> _valueGetters;//store column value getter expression 
        readonly StringBuilder _selectSqlBuffer;

        Projection(string fullSelection, Delegate identitySelector)
        {
            this.IsProjection = false;
            this.Projector = identitySelector;
            this.SelectSql = fullSelection;
        }

        Projection(Table.Info tableInfo, LambdaExpression selector)
        {
            this._valueGetters = new Dictionary<string, Expression>();
            this._selectSqlBuffer = new StringBuilder();
            this._tableInfo = tableInfo;
            this.IsProjection = true;
            this._paramT = selector.Parameters[0];
            var projector = Expression.Lambda(base.Visit(selector.Body), _DbReader);
            var lambda = this.IsProjection ? projector : selector;
            this.Projector = lambda.Compile();
            this.SelectSql = IsProjection ? _selectSqlBuffer.ToString() : tableInfo.FullSelectionColumnsString;

            this._selectSqlBuffer.Length = 0;
            this._selectSqlBuffer = null;
            this._valueGetters.Clear();
            this._valueGetters = null;
            this._tableInfo = null;
            this._paramT = null;
        }

        internal static Projection Identity<T>() where T : DbEntity
        {
            return new Projection(Table<T>.FullSelectionColumnsString, new Func<T, T>(t => t));
        }

        protected override Expression VisitMember(MemberExpression m)
        {
            if (!IsProjection)
                return m;

            if (m.Expression == null || m.Expression.Type != _paramT.Type)
                return base.VisitMember(m);

            var c = _tableInfo.DbColumnMap[m.Member.Name];
            Expression expr;
            if (_valueGetters.TryGetValue(c.ColumnName, out expr))
                return expr;

            //append select column
            if (_selectSqlBuffer.Length > 0)
                _selectSqlBuffer.Append(',');
            _selectSqlBuffer.Append(c.TableQuotesColumnName);

            // reader.GetOraXXX(index); extension method
            var mi = OracleDataReaderHelper.GetValueGetMethod(m.Type, c.DbType, c.IsNullable);
            expr = Expression.Call(mi, _DbReader, Expression.Constant(_valueGetters.Count));
            _valueGetters.Add(c.ColumnName, expr);
            return expr;
        }

        protected override Expression VisitParameter(ParameterExpression p)
        {
            if (IsProjection && p == _paramT)
                IsProjection = false;
            return p;
        }
    }
}
