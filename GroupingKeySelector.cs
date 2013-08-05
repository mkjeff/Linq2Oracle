using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Oracle.ManagedDataAccess.Client;

namespace Linq2Oracle {
    /// <summary>
    /// from ...
    /// group . by . into g
    /// select new { 
    ///     /* 
    ///         Grouping Aggregate function :g 
    ///     */ 
    /// }
    /// </summary>
    sealed class GroupingAggregate : ExpressionVisitor {
        static readonly ParameterExpression dbReader = Expression.Parameter(typeof(OracleDataReader), "reader");
        static readonly ExpressionCache<GroupingAggregate> _Cache = new ExpressionCache<GroupingAggregate>();
        static GroupingAggregate() { }

        public readonly string SelectionSql;
        public readonly Delegate ValueSelector;
        public readonly GroupingKeySelector GrouipingKeySelector;
        readonly StringBuilder _selectionSqlBuffer = new StringBuilder();
        readonly ParameterExpression _paramT;
        readonly PropertyInfo _keyMember;
        readonly Table.Info _tableInfo;
        readonly Dictionary<object, Expression> _valueGetters;//store column value getter expression 

        internal static GroupingAggregate Create<T, TKey, TResult>(GroupingKeySelector keySelector, Expression<Func<IGroupingContext<T, TKey>, TResult>> resultSelector) where T : DbEntity
        {
            return _Cache.Get(resultSelector, key => new GroupingAggregate(Table<T>.Info, keySelector, key));
        }

        GroupingAggregate(Table.Info tableInfo, GroupingKeySelector keySelector, LambdaExpression valueSelector) {
            this._valueGetters = new Dictionary<object, Expression>();//store column value getter expression
            this._tableInfo = tableInfo;
            this._paramT = valueSelector.Parameters[0];
            this.GrouipingKeySelector = keySelector;
            this._keyMember = _paramT.Type.GetProperty("Key");
            var lambda = Expression.Lambda(base.Visit(valueSelector.Body), dbReader);
            this.ValueSelector = lambda.Compile();
            this.SelectionSql = _selectionSqlBuffer.ToString();

            this._selectionSqlBuffer.Length = 0;
            this._selectionSqlBuffer = null;
            this._tableInfo = null;
            this._valueGetters.Clear();
            this._valueGetters = null;
            this._paramT = null;
            this._keyMember = null;
        }

        protected override Expression VisitMemberAccess(MemberExpression m) {
            if (m.Expression == null)
                return base.VisitMemberAccess(m);

            Expression expr = null;
            if (_valueGetters.TryGetValue(m.Member, out expr))
                return expr;

            // select 整個 Key 當作輸出 而不是存取 Key.Property,
            // 需要重新Vist GroupBy's key selector expression
            if (m.Member == _keyMember) 
                return new KeySelectorVisitor(_tableInfo, _selectionSqlBuffer, GrouipingKeySelector.KeySelectorExpression, _valueGetters).Expression;

            if (m.Expression.Type == GrouipingKeySelector.KeySelectorExpression.Body.Type) {
                // visit Key.property
                var c = GrouipingKeySelector.GetColumn((PropertyInfo)m.Member);// columnMap[m.Member.Name];
                // reader.GetOraXXX(index); extension method
                if (_selectionSqlBuffer.Length > 0)
                    _selectionSqlBuffer.Append(',');
                _selectionSqlBuffer.Append(c.TableQuotesColumnName);
                expr =  Expression.Call(OracleDataReaderHelper.GetValueGetMethod(m.Type, c.DbType, c.IsNullable), dbReader, Expression.Constant(_valueGetters.Count));
                _valueGetters.Add(m.Member,expr);
                return expr;
            }
            return base.VisitMemberAccess(m);
        }

        protected override Expression VisitMethodCall(MethodCallExpression m) {
            if (m.Method.DeclaringType != _paramT.Type)
                return base.VisitMethodCall(m);

            string key = m.ToString();
            Expression expr = null;
            if (_valueGetters.TryGetValue(key, out expr))
                return expr;

            DbColumn c = null;
            if (m.Arguments.Any())
                c = _tableInfo.DbColumnMap[new AggregateColumnVisitor(m.Arguments[0]).ColumnName];

            string funcExpr = null;
            switch (m.Method.Name) {
                case "Sum": // g.Sum(a=> ...)
                    funcExpr = "SUM({0})";
                    break;
                case "Average": // g.Average(a=> ...)
                    funcExpr = "ROUND(AVG({0}),12)";
                    break;
                case "Max": // g.Max(a=> ...)
                    funcExpr = "MAX({0})";
                    break;
                case "Min": // g.Min(a=> ...)
                    funcExpr = "MIN({0})";
                    break;
                case "Count": // g.Count()
                    funcExpr = "COUNT(*)";
                    break;
                default:
                    throw new DalException(DbErrorCode.E_DB_NOT_SUPPORT_OPERATOR, "不支援" + m.ToString());
            }
            if (_selectionSqlBuffer.Length > 0)
                _selectionSqlBuffer.Append(',');

            MethodInfo mi = null;

            if (c == null) { // COUNT(*)
                _selectionSqlBuffer.Append(funcExpr);
                mi = OracleDataReaderHelper.GetValueGetMethod(m.Type, OracleDbType.Int64, false);
            } else {
                _selectionSqlBuffer.AppendFormat(funcExpr, c.TableQuotesColumnName);
                mi = OracleDataReaderHelper.GetValueGetMethod(m.Type, c.DbType, c.IsNullable);
            }

            expr = Expression.Call(mi, dbReader, Expression.Constant(_valueGetters.Count));
            _valueGetters.Add(key, expr);
            return expr;
        }

        #region Inner Type
        sealed class AggregateColumnVisitor : ExpressionVisitor {
            public string ColumnName { get; private set; }
            internal AggregateColumnVisitor(Expression expr) {
                base.Visit(expr);
            }

            protected override Expression VisitMemberAccess(MemberExpression m) {
                ColumnName = m.Member.Name;
                return m;
            }
        }

        sealed class KeySelectorVisitor : ExpressionVisitor {
            readonly StringBuilder selection;
            readonly Table.Info tableInfo;
            readonly LambdaExpression Lambda;
            public readonly Expression Expression;
            readonly Dictionary<object, Expression> valueGetterMap;
            internal KeySelectorVisitor(Table.Info tableInfo, StringBuilder selection, LambdaExpression lambda, Dictionary<object, Expression> valueGetterMap) {
                this.valueGetterMap = valueGetterMap;
                this.Lambda = lambda;
                this.tableInfo = tableInfo;
                this.selection = selection;
                this.Expression = base.Visit(Lambda.Body);
                this.Lambda = null;
                this.tableInfo = null;
            }

            protected override Expression VisitMemberInit(MemberInitExpression init) {
                // new Class{ Member init }
                throw new DalException(DbErrorCode.E_DB_NOT_SUPPORT_OPERATOR, "不支援" + init.ToString());
            }

            protected override Expression VisitMethodCall(MethodCallExpression m) {
                throw new DalException(DbErrorCode.E_DB_NOT_SUPPORT_OPERATOR, "不支援" + m.ToString());
            }

            IEnumerable<Expression> ParseNew(NewExpression nex) {
                for (int i = 0; i < nex.Arguments.Count; i++)
                {
                    var arg = nex.Arguments[i] as MemberExpression;
                    var pi = (PropertyInfo)nex.Members[i];// Lambda.Body.Type.GetProperty(mi.Name.Substring(mi.Name.IndexOf('_') + 1));

                    Expression getter = null;
                    if (valueGetterMap.TryGetValue(pi, out getter))
                    {
                        yield return getter;
                        continue;
                    }

                    var c = this.tableInfo.DbColumnMap[arg.Member.Name];

                    if (selection.Length > 0)
                        selection.Append(',');
                    selection.Append(c.TableQuotesColumnName);
                    getter = Expression.Call(OracleDataReaderHelper.GetValueGetMethod(arg.Type, c.DbType, c.IsNullable), dbReader, Expression.Constant(valueGetterMap.Count));
                    valueGetterMap.Add(pi, getter);
                    yield return getter;
                }
            }

            protected override NewExpression VisitNew(NewExpression nex) {
                return Expression.New(nex.Constructor, ParseNew(nex), nex.Members);
            }

            protected override Expression VisitMemberAccess(MemberExpression m) {
                if (m.Expression.Type != Lambda.Parameters[0].Type)
                    throw new DalException(DbErrorCode.E_DB_NOT_SUPPORT_OPERATOR, "不支援" + m.ToString());

                Expression expr = null;
                if (valueGetterMap.TryGetValue(m.Member, out expr))
                    return expr;
                var c = this.tableInfo.DbColumnMap[m.Member.Name];
                // reader.GetOraXXX(index); extension method
                if (selection.Length > 0)
                    selection.Append(',');
                selection.Append(c.TableQuotesColumnName);
                expr = Expression.Call(OracleDataReaderHelper.GetValueGetMethod(m.Type, c.DbType, c.IsNullable), dbReader, Expression.Constant(valueGetterMap.Count));
                valueGetterMap.Add(m.Member,expr);
                return expr;
            }
        }
        #endregion
    }

    /// <summary>
    /// translate flowing query
    ///     from ...
    ///     group by {KeySelectorExpression}
    /// 
    /// into SQL
    ///     select ..
    ///     from ...
    ///     group {GroupKeySql}
    /// </summary>
    sealed class GroupingKeySelector : ExpressionVisitor {
        static readonly ExpressionCache<GroupingKeySelector> _Cache = new ExpressionCache<GroupingKeySelector>();
        public readonly LambdaExpression KeySelectorExpression;
        public readonly string GroupKeySql;
        readonly Table.Info _tableInfo;
        Dictionary<PropertyInfo, DbColumn> _memberMap;

        GroupingKeySelector(Table.Info tableInfo, LambdaExpression keySelector) {
            this._tableInfo = tableInfo;
            this.KeySelectorExpression = keySelector;
            base.Visit(keySelector.Body);

            GroupKeySql = string.Join(",", _memberMap.Values.Select(c => c.TableQuotesColumnName).ToArray());

            this._tableInfo = null;
        }

        internal DbColumn GetColumn(PropertyInfo property) {
            return _memberMap[property];
        }

        internal static GroupingKeySelector Create<T, TKey>(Expression<Func<T, TKey>> keySelector) where T : DbEntity {
            return _Cache.Get(keySelector, key => new GroupingKeySelector(Table<T>.Info, key));
        }

        internal Predicate GetGroupKeyPredicate<TKey>(TKey groupKey) {
            return new Predicate((sql, param) => {
                int i = 0;
                foreach (var c in _memberMap.Values) {
                    var value = c.GetDbValue(groupKey);
                    if (i++ != 0)
                        sql.Append(" AND ");
                    sql.Append(c.TableQuotesColumnName);
                    if (value == DBNull.Value)
                        sql.Append("IS NULL");
                    else
                        sql.Append(" = ").AppendParam(param, c.DbType, c.Size, value);
                }
            });
        }

        protected override Expression VisitMemberInit(MemberInitExpression init) {
            // group by new Class{ Member init }
            throw new DalException(DbErrorCode.E_DB_NOT_SUPPORT_OPERATOR, "不支援" + init.ToString());
        }

        protected override NewExpression VisitNew(NewExpression nex) {
            //throw new DalException(DbErrorCode.E_DB_NOT_SUPPORT_OPERATOR, "不支援巢狀類別的Key" + nex.ToString());
            _memberMap = nex.Arguments.Cast<MemberExpression>()
                .Zip(nex.Members, (arg, member) => new
                {
                    Property = member as PropertyInfo, /* in 3.5 member is Property get MethodInfo */
                    ColumnName = arg.Member.Name
                })
                .ToDictionary(
                    a => a.Property, 
                    a => new DbColumn(a.Property, this._tableInfo.DbColumnMap[a.ColumnName])
                );

            return nex;
        }

        protected override Expression VisitMemberAccess(MemberExpression m) {
            if (m.Expression == null || m.Expression.Type != KeySelectorExpression.Parameters[0].Type)
                throw new DalException(DbErrorCode.E_DB_NOT_SUPPORT_OPERATOR, "不支援" + m.ToString());

            var c = this._tableInfo.DbColumnMap[m.Member.Name];
            _memberMap = new Dictionary<PropertyInfo, DbColumn>
            {
                {(PropertyInfo)m.Member , new DbColumn((PropertyInfo)m.Member, c)}
            };
            return m;
        }

        protected override Expression VisitMethodCall(MethodCallExpression m) {
            throw new DalException(DbErrorCode.E_DB_NOT_SUPPORT_OPERATOR, "不支援" + m.ToString());
        }
    }
}
