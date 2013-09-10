using Linq2Oracle.Expressions;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Linq2Oracle
{
    public sealed class SqlContext
    {
        public int ParameterCount { get { return param.Count; } }

        readonly StringBuilder sql;
        readonly OracleParameterCollection param;
        readonly Dictionary<IQueryContext, string> _alias = new Dictionary<IQueryContext, string>();

        public override string ToString()
        {
            return sql.ToString();
        }

        internal SqlContext(StringBuilder sql, OracleParameterCollection param)
        {
            this.sql = sql;
            this.param = param;
        }

        internal string GetAlias(IQueryContext query)
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

        internal SqlContext Append(IQueryContext subQuery)
        {
            subQuery.GenInnerSql(this);
            return this;
        }

        internal SqlContext Append(string selection, IQueryContext subQuery)
        {
            subQuery.GenInnerSql(this, selection);
            return this;
        }

        internal SqlContext Append(string str)
        {
            sql.Append(str);
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

        internal SqlContext Append(IDbExpression expression)
        {
            if (expression == null)
                throw new ArgumentNullException("expression");

            expression.Build(this);
            return this;
        }

        internal SqlContext AppendParam(OracleDbType dbType, object value)
        {
            sql.Append(':').Append(param.Add(param.Count.ToString(), dbType, value, ParameterDirection.Input).ParameterName);
            return this;
        }

        internal SqlContext AppendParam(object value)
        {
            sql.Append(':').Append(param.Add(param.Count.ToString(), value).ParameterName);
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
                    sql.Append(delimiter).Append(filter);
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
                    sql.Append(delimiter);
                    this.Append(order.Expression);
                    if (order.Descending)
                        sql.Append(" DESC");
                    delimiter = ", ";
                }
            }
            return this;
        }
    }
}
