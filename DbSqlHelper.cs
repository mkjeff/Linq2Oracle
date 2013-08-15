using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;

namespace Linq2Oracle {
    public static class DbSqlHelper {
        #region Internal Members
        internal static T Init<T>(this T column, string expression, IDbMetaInfo columnInfo) where T : IDbExpression
        {
            column.SetColumnInfo(expression, columnInfo);
            return column;
        }

        internal static StringBuilder AppendParam(this StringBuilder sql, OracleParameterCollection param, OracleDbType dbType, int size, object value) {
            return sql.Append(':').Append(param.Add(param.Count.ToString(), dbType, size, value, ParameterDirection.Input).ParameterName);
        }

        internal static StringBuilder AppendForUpdate<T, TResult>(this StringBuilder sql, int? updateWait) where T : DbEntity {
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

        internal static void JoinString<T>(this T[] list, Func<T, string> stringSelector, StringBuilder sb, string separator) {
            for (int i = 0, cnt = list.Length; i < cnt; i++) {
                if (i != 0)
                    sb.Append(separator);
                sb.Append(stringSelector(list[i]));
            }
        }

        internal static R[] ConvertAll<T, R>(this T[] array, Converter<T, R> converter) {
            return Array.ConvertAll(array, converter);
        }

        internal static StringBuilder MappingAlias(this StringBuilder sql, int startIndex, IEnumerable<IQueryContext> mapping) {
            int i = 0;
            foreach (var alias in mapping)
                sql.Replace(alias.TableName + ".", "t" + i++ + ".", startIndex, sql.Length - startIndex);
            return sql;
        }

        internal static StringBuilder MappingAlias(this StringBuilder sql, int startIndex, IQueryContext mapping) {
            return sql.Replace(mapping.TableName + ".", "t0.", startIndex, sql.Length - startIndex);
        }

        internal static object ToDbValue(this object value) {
            if (value == null)
                return DBNull.Value;

            if (value is string)
                return value;

            Type vType = value.GetType();

            if (vType.IsEnum)
                return Enum.GetName(vType, value);

            if (vType.IsGenericType && (vType.GetGenericTypeDefinition() == typeof(Nullable<>))) {
                vType = vType.GetGenericArguments()[0];
                if (vType.IsEnum)
                    return Enum.GetName(vType, value);
            }

            return value;
        }

        internal static StringBuilder AppendWhere(this StringBuilder sql, OracleParameterCollection param, IEnumerable<Predicate> filters) {
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

        internal static StringBuilder AppendOrder(this StringBuilder sql, IEnumerable<SortDescription> orders) {
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
                // 所以將超過1000筆的IN敘述拆解成columnName IN(...) OR columnName IN(...)條件
                const int size = 1000;
                int pageNum = (int)Math.Ceiling((double)values.Length / size);

                for (int p = 0; p < pageNum; p++)
                {
                    if (p != 0) sql.Append(" OR ");

                    sql.Append(@this.Expression).Append(" IN (");
                    for (int i = p * size, offset = (p + 1) * size; i < offset && i < values.Length; i++)
                    {
                        if (values[i] == null)
                            sql.Append("NULL");
                        else
                            sql.AppendParam(param, @this.DbType, @this.Size, values[i].ToDbValue());
                        sql.Append(',');
                    }

                    sql.Remove(sql.Length - 1, 1).Append(')');
                }
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
        public static Predicate In<C1, C2, T1, T2>(this Tuple<C1, C2> columns, params Tuple<T1, T2>[] values)
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

                sql.Append('(').Append(columns.Item1.Expression).Append(',').Append(columns.Item2.Expression)
                   .Append(") IN (");

                for (int i = 0, cnt = values.Length; i < cnt; i++)
                {
                    if (i != 0) sql.Append(',');
                    var t = values[i];

                    sql.Append('(');

                    if (t.Item1 == null) sql.Append("NULL");
                    else sql.AppendParam(param, columns.Item1.DbType, columns.Item1.Size, t.Item1.ToDbValue());

                    sql.Append(',');

                    if (t.Item2 == null) sql.Append("NULL");
                    else sql.AppendParam(param, columns.Item2.DbType, columns.Item2.Size, t.Item2.ToDbValue());

                    sql.Append(')');
                }

                sql.Append(")");
            });
        }

        public static Predicate In<C1, C2, T1, T2>(this Tuple<C1, C2> columns, IEnumerable<Tuple<T1, T2>> values)
            where C1 : DbExpression<T1>
            where C2 : DbExpression<T2>
        {
            return columns.In(values.ToArray());
        }

        public static Predicate In<C1, C2, T1, T2>(this Tuple<C1, C2> columns, IQueryContext<Tuple<T1, T2>> subquery)
            where C1 : DbExpression<T1>
            where C2 : DbExpression<T2>
        {
            return new Predicate((sql, param) =>
            {
                sql.Append('(')
                   .Append(columns.Item1.Expression)
                   .Append(',')
                   .Append(columns.Item2.Expression)
                   .Append(") IN (");
                subquery.GenInnerSql(sql, param);
                sql.Append(')');
            });
        }
        #endregion
        #region Where (Column1,Column2,Column3) In (...)
        public static Predicate In<C1, C2, C3, T1, T2, T3>(this Tuple<C1, C2, C3> columns, params Tuple<T1, T2, T3>[] values)
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
                    .Append(columns.Item1.Expression).Append(',')
                    .Append(columns.Item2.Expression).Append(',')
                    .Append(columns.Item3.Expression)
                .Append(") IN (");
                for (int i = 0, cnt = values.Length; i < cnt; i++)
                {
                    if (i != 0) sql.Append(',');
                    var t = values[i];
                    sql.Append('(');

                    if (t.Item1 == null) sql.Append("NULL");
                    else sql.AppendParam(param, columns.Item1.DbType, columns.Item1.Size, t.Item1.ToDbValue());

                    sql.Append(',');

                    if (t.Item2 == null) sql.Append("NULL");
                    else sql.AppendParam(param, columns.Item2.DbType, columns.Item2.Size, t.Item2.ToDbValue());

                    sql.Append(',');

                    if (t.Item3 == null) sql.Append("NULL");
                    else sql.AppendParam(param, columns.Item3.DbType, columns.Item3.Size, t.Item3.ToDbValue());

                    sql.Append(')');
                }
                sql.Append(')');
            });
        }

        public static Predicate In<C1, C2, C3, T1, T2, T3>(this Tuple<C1, C2, C3> columns, IEnumerable<Tuple<T1, T2, T3>> values)
            where C1 : DbExpression<T1>
            where C2 : DbExpression<T2>
            where C3 : DbExpression<T3>
        {
            return columns.In(values.ToArray());
        }

        public static Predicate In<C1, C2, C3, T1, T2, T3>(this Tuple<C1, C2, C3> columns, IQueryContext<Tuple<T1, T2, T3>> subquery)
            where C1 : DbExpression<T1>
            where C2 : DbExpression<T2>
            where C3 : DbExpression<T3>
        {
            return new Predicate((sql, param) =>
            {
                sql.Append('(')
                   .Append(columns.Item1.Expression)
                   .Append(',')
                   .Append(columns.Item2.Expression)
                   .Append(',')
                   .Append(columns.Item3.Expression)
                   .Append(") IN (");
                subquery.GenInnerSql(sql, param);
                sql.Append(')');
            });
        }
        #endregion
    }
}
