using Linq2Oracle.Expressions;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Linq2Oracle
{
    public sealed class SqlContext
    {
        public int ParameterCount => param.Count;

        readonly StringBuilder sql;
        readonly OracleParameterCollection param;
        readonly Dictionary<IQueryContext, string> _alias = new Dictionary<IQueryContext, string>();

        public override string ToString() => sql.ToString();

        internal SqlContext(StringBuilder sql, OracleParameterCollection param)
        {
            this.sql = sql;
            this.param = param;
        }

        public string GetAlias(IQueryContext query)
        {
            string alias = null;
            if (_alias.TryGetValue(query.OriginalSource, out alias))
                return alias;
            alias = "t" + _alias.Count;
            _alias.Add(query.OriginalSource, alias);
            return alias;
        }

        internal SqlContext MappingAlias(IQueryContext query)
        {
            sql.Replace(query.TableName + ".", GetAlias(query) + ".");
            return this;
        }

        internal SqlContext Append(SqlBoolean predicate)
        {
            predicate.Build(this);
            return this;
        }

        internal SqlContext AppendQuery(IQueryContext subQuery)
        {
            subQuery.GenInnerSql(this);
            return this;
        }

        internal SqlContext Append(string selection, IQueryContext subQuery)
        {
            subQuery.GenInnerSql(this, selection);
            return this;
        }

        public SqlContext Append(string str)
        {
            sql.Append(str);
            return this;
        }

        internal SqlContext Append(Action<SqlContext> sqlGenetor)
        {
            sqlGenetor(this);
            return this;
        }

        internal SqlContext Append(char ch)
        {
            sql.Append(ch);
            return this;
        }

        internal SqlContext Append(int integer)
        {
            sql.Append(integer);
            return this;
        }

        internal SqlContext Append<T>(T expression) where T : IDbExpression
        {
            if (expression.IsNullExpression)
                sql.Append("NULL");
            else
                expression.Build(this);
            return this;
        }

        internal SqlContext AppendParam(OracleDbType dbType, object value)
        {
            var paramName = param.Count.ToString();
            param.Add(paramName, dbType, value, ParameterDirection.Input);
            sql.Append(':').Append(paramName);
            return this;
        }

        internal SqlContext AppendParam<T>(T value)
        {
            var paramName = param.Count.ToString();
            if (value == null)
            {
                sql.Append("NULL");
            }
            else
            {
                if (typeof(T).IsEnum)
                    param.Add(paramName, OracleDbType.Varchar2, value, ParameterDirection.Input);
                else
                    param.Add(paramName, value);
                sql.Append(':').Append(paramName);
            }
            return this;
        }

        internal void AppendForUpdate<T, TResult>(int? updateWait) where T : DbEntity
        {
            if (!updateWait.HasValue)
                return;
            if (typeof(T) != typeof(TResult))
                return;
            if (updateWait.Value == 0)
                sql.Append(" FOR UPDATE NOWAIT");
            else if (updateWait.Value > 0)
                sql.Append(" FOR UPDATE WAIT ").Append(updateWait.Value);
            else
                sql.Append(" FOR UPDATE SKIP LOCKED");
        }

        internal SqlContext AppendWhere(IEnumerable<SqlBoolean> filters)
        {
            if (filters.Any())
            {
                sql.Append(" WHERE ");
                string delimiter = string.Empty;
                foreach (var filter in filters)
                {
                    this.Append(delimiter).Append(filter);
                    delimiter = " AND ";
                }
            }
            return this;
        }

        internal SqlContext AppendHaving(IEnumerable<SqlBoolean> filters)
        {
            if (filters.Any())
            {
                sql.Append(" HAVING ");
                string delimiter = string.Empty;
                foreach (var filter in filters)
                {
                    this.Append(delimiter).Append(filter);
                    delimiter = " AND ";
                }
            }
            return this;
        }

        internal SqlContext AppendOrder(IEnumerable<SortDescription> orders)
        {
            if (orders.Any())
            {
                sql.Append(" ORDER BY ");
                string delimiter = string.Empty;
                foreach (var order in orders)
                {
                    Append(delimiter).Append(order.Expression).Append(order.Descending? " DESC":string.Empty);
                    delimiter = ", ";
                }
            }
            return this;
        }
    }
}
