using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Linq2Oracle
{
    public static class DbSqlHelper
    {
        #region Internal Members
        internal static T Init<T>(this T column, string expression, IDbMetaInfo columnInfo) where T : IDbExpression
        {
            column.SetColumnInfo(expression, columnInfo);
            return column;
        }

        internal static StringBuilder AppendParam(this StringBuilder sql, OracleParameterCollection param, OracleDbType dbType, object value)
        {
            return sql.Append(':').Append(param.Add(param.Count.ToString(), dbType, value, ParameterDirection.Input).ParameterName);
        }

        internal static StringBuilder AppendForUpdate<T, TResult>(this StringBuilder sql, int? updateWait) where T : DbEntity
        {
            if (!updateWait.HasValue)
                return sql;
            if (typeof(T) != typeof(TResult))
                return sql;
            if (updateWait.Value == 0)
                return sql.Append(" FOR UPDATE NOWAIT");
            if (updateWait.Value > 0)
                return sql.Append(" FOR UPDATE WAIT ").Append(updateWait.Value);
            return sql.Append(" FOR UPDATE SKIP LOCKED");
        }

        internal static R[] ConvertAll<T, R>(this T[] array, Converter<T, R> converter)
        {
            return Array.ConvertAll(array, converter);
        }

        internal static StringBuilder MappingAlias(this StringBuilder sql, int startIndex, IEnumerable<IQueryContext> mapping)
        {
            int i = 0;
            foreach (var alias in mapping)
                sql.Replace(alias.TableName + ".", "t" + i++ + ".", startIndex, sql.Length - startIndex);
            return sql;
        }

        internal static StringBuilder MappingAlias(this StringBuilder sql, int startIndex, IQueryContext mapping)
        {
            return sql.Replace(mapping.TableName + ".", "t0.", startIndex, sql.Length - startIndex);
        }

        internal static StringBuilder AppendWhere(this StringBuilder sql, OracleParameterCollection param, IEnumerable<Predicate> filters)
        {
            if (filters.IsEmpty())
                return sql;
            sql.Append(" WHERE ");
            string delimiter = string.Empty;
            foreach (var filter in filters)
            {
                sql.Append(delimiter);
                filter.Build(sql, param);
                delimiter = " AND ";
            }
            return sql;
        }

        internal static StringBuilder AppendHaving(this StringBuilder sql, OracleParameterCollection param, IEnumerable<Predicate> filters)
        {
            if (filters.IsEmpty())
                return sql;
            sql.Append(" HAVING ");
            string delimiter = string.Empty;
            foreach (var filter in filters)
            {
                sql.Append(delimiter);
                filter.Build(sql, param);
                delimiter = " AND ";
            }
            return sql;
        }

        internal static StringBuilder AppendOrder(this StringBuilder sql, IEnumerable<SortDescription> orders)
        {
            if (orders.IsEmpty())
                return sql;
            sql.Append(" ORDER BY ");
            string delimiter = string.Empty;
            foreach (var order in orders)
            {
                sql.Append(delimiter);
                sql.Append(order.Expression);
                if (order.Descending)
                    sql.Append(" DESC");
                delimiter = ", ";
            }
            return sql;
        }
        #endregion

        #region Where Column In (...)
        public static Predicate In<T>(this DbExpression<T> @this, IEnumerable<T> values)
        {
            return @this.In(values.ToArray());
        }
        public static Predicate In<T>(this DbExpression<T> @this, params T[] values)
        {
            return new Predicate((sql, param) =>
            {
                if (values.Length == 0)
                {
                    sql.Append("1=2");
                    return;
                }

                // Oracle SQL有限制IN(...) list大小不能超過1000筆, 
                // 如果筆數太多應該考慮使用其他查詢條件

                sql.Append(@this.Expression).Append(" IN (");

                string delimiter = string.Empty;
                foreach (var t in values)
                {
                    sql.Append(delimiter);

                    if (t == null)
                        sql.Append("NULL");
                    else
                        sql.AppendParam(param, @this.DbType, t);
                    delimiter = ", ";
                }

                sql.Append(')');
            });
        }

        public static Predicate In<T>(this DbExpression<T> @this, IQueryContext<T> subquery)
        {
            return new Predicate((sql, param) =>
            {
                sql.Append(@this.Expression).Append(" IN (");
                subquery.GenInnerSql(sql, param);
                sql.Append(')');
            });
        }
        #endregion
        #region Where (Column1,Column2) In (...)
        public static Predicate In<C1, C2, T1, T2>(this Tuple<C1, C2> @this, params Tuple<T1, T2>[] values)
            where C1 : DbExpression<T1>
            where C2 : DbExpression<T2>
        {
            return new Predicate((sql, param) =>
            {
                if (values.Length == 0)
                {
                    sql.Append("1=2");
                    return;
                }

                sql.Append('(')
                    .Append(@this.Item1.Expression).Append(',')
                    .Append(@this.Item2.Expression)
                   .Append(") IN (");

                string delimiter = string.Empty;
                foreach (var t in values)
                {
                    sql.Append(delimiter).Append('(');

                    if (t.Item1 == null) sql.Append("NULL");
                    else sql.AppendParam(param, @this.Item1.DbType, t.Item1);

                    sql.Append(',');

                    if (t.Item2 == null) sql.Append("NULL");
                    else sql.AppendParam(param, @this.Item2.DbType, t.Item2);

                    sql.Append(')');

                    delimiter = ", ";
                }

                sql.Append(")");
            });
        }

        public static Predicate In<C1, C2, T1, T2>(this Tuple<C1, C2> @this, IEnumerable<Tuple<T1, T2>> values)
            where C1 : DbExpression<T1>
            where C2 : DbExpression<T2>
        {
            return @this.In(values.ToArray());
        }

        public static Predicate In<C1, C2, T1, T2>(this Tuple<C1, C2> @this, IQueryContext<Tuple<T1, T2>> subquery)
            where C1 : DbExpression<T1>
            where C2 : DbExpression<T2>
        {
            return new Predicate((sql, param) =>
            {
                sql.Append('(')
                   .Append(@this.Item1.Expression)
                   .Append(',')
                   .Append(@this.Item2.Expression)
                   .Append(") IN (");
                subquery.GenInnerSql(sql, param);
                sql.Append(')');
            });
        }
        #endregion
        #region Where (Column1,Column2,Column3) In (...)
        public static Predicate In<C1, C2, C3, T1, T2, T3>(this Tuple<C1, C2, C3> @this, params Tuple<T1, T2, T3>[] values)
            where C1 : DbExpression<T1>
            where C2 : DbExpression<T2>
            where C3 : DbExpression<T3>
        {
            return new Predicate((sql, param) =>
            {
                if (values.Length == 0)
                {
                    sql.Append("1=2");
                    return;
                }

                sql.Append('(')
                    .Append(@this.Item1.Expression).Append(',')
                    .Append(@this.Item2.Expression).Append(',')
                    .Append(@this.Item3.Expression)
                .Append(") IN (");

                string delimiter = string.Empty;
                foreach (var t in values)
                {
                    sql.Append(delimiter).Append('(');

                    if (t.Item1 == null) sql.Append("NULL");
                    else sql.AppendParam(param, @this.Item1.DbType, t.Item1);

                    sql.Append(',');

                    if (t.Item2 == null) sql.Append("NULL");
                    else sql.AppendParam(param, @this.Item2.DbType, t.Item2);

                    sql.Append(',');

                    if (t.Item3 == null) sql.Append("NULL");
                    else sql.AppendParam(param, @this.Item3.DbType, t.Item3);

                    sql.Append(')');
                    delimiter = ", ";
                }
                sql.Append(')');
            });
        }

        public static Predicate In<C1, C2, C3, T1, T2, T3>(this Tuple<C1, C2, C3> @this, IEnumerable<Tuple<T1, T2, T3>> values)
            where C1 : DbExpression<T1>
            where C2 : DbExpression<T2>
            where C3 : DbExpression<T3>
        {
            return @this.In(values.ToArray());
        }

        public static Predicate In<C1, C2, C3, T1, T2, T3>(this Tuple<C1, C2, C3> @this, IQueryContext<Tuple<T1, T2, T3>> subquery)
            where C1 : DbExpression<T1>
            where C2 : DbExpression<T2>
            where C3 : DbExpression<T3>
        {
            return new Predicate((sql, param) =>
            {
                sql.Append('(')
                   .Append(@this.Item1.Expression)
                   .Append(',')
                   .Append(@this.Item2.Expression)
                   .Append(',')
                   .Append(@this.Item3.Expression)
                   .Append(") IN (");
                subquery.GenInnerSql(sql, param);
                sql.Append(')');
            });
        }
        #endregion
    }
}
