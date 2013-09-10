using Linq2Oracle.Expressions;
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

        internal static StringBuilder AppendParam(this StringBuilder sql, OracleParameterCollection param, OracleDbType dbType, object value)
        {
            return sql.Append(':').Append(param.Add(param.Count.ToString(), dbType, value, ParameterDirection.Input).ParameterName);
        }

        internal static R[] ConvertAll<T, R>(this T[] array, Converter<T, R> converter)
        {
            return Array.ConvertAll(array, converter);
        }

        #endregion

        #region Where Column In (...)
        public static SqlBoolean In<T>(this IDbExpression<T> @this, IEnumerable<T> values)
        {
            return @this.In(values.ToArray());
        }
        public static SqlBoolean In<T>(this IDbExpression<T> @this, params T[] values)
        {
            return new SqlBoolean(sql =>
            {
                if (values.Length == 0)
                {
                    sql.Append("1=2");
                    return;
                }

                // Oracle SQL有限制IN(...) list大小不能超過1000筆, 
                // 如果筆數太多應該考慮使用其他查詢條件

                sql.Append(@this).Append(" IN (");

                string delimiter = string.Empty;
                foreach (var t in values)
                {
                    sql.Append(delimiter);

                    if (t == null)
                        sql.Append("NULL");
                    else
                        sql.AppendParam(@this.DbType, t);
                    delimiter = ", ";
                }

                sql.Append(')');
            });
        }

        public static SqlBoolean In<T>(this IDbExpression<T> @this, IQueryContext<T> subquery)
        {
            return new SqlBoolean(sql => sql.Append(@this).Append(" IN (").Append(subquery).Append(')'));
        }
        #endregion
        #region Where (Column1,Column2) In (...)
        public static SqlBoolean In<C1, C2, T1, T2>(this Tuple<C1, C2> @this, params Tuple<T1, T2>[] values)
            where C1 : IDbExpression<T1>
            where C2 : IDbExpression<T2>
        {
            return new SqlBoolean(sql =>
            {
                if (values.Length == 0)
                {
                    sql.Append("1=2");
                    return;
                }

                sql.Append('(').Append(@this.Item1).Append(',').Append(@this.Item2).Append(") IN (");

                string delimiter = string.Empty;
                foreach (var t in values)
                {
                    sql.Append(delimiter).Append('(');

                    if (t.Item1 == null) sql.Append("NULL");
                    else sql.AppendParam(@this.Item1.DbType, t.Item1);

                    sql.Append(',');

                    if (t.Item2 == null) sql.Append("NULL");
                    else sql.AppendParam(@this.Item2.DbType, t.Item2);

                    sql.Append(')');

                    delimiter = ", ";
                }

                sql.Append(")");
            });
        }

        public static SqlBoolean In<C1, C2, T1, T2>(this Tuple<C1, C2> @this, IEnumerable<Tuple<T1, T2>> values)
            where C1 : IDbExpression<T1>
            where C2 : IDbExpression<T2>
        {
            return @this.In(values.ToArray());
        }

        public static SqlBoolean In<C1, C2, T1, T2>(this Tuple<C1, C2> @this, IQueryContext<Tuple<T1, T2>> subquery)
            where C1 : IDbExpression<T1>
            where C2 : IDbExpression<T2>
        {
            return new SqlBoolean(sql =>
                sql.Append('(')
                    .Append(@this.Item1).Append(',')
                    .Append(@this.Item2).Append(") IN (")
                    .Append(subquery)
                    .Append(')'));
        }
        #endregion
        #region Where (Column1,Column2,Column3) In (...)
        public static SqlBoolean In<C1, C2, C3, T1, T2, T3>(this Tuple<C1, C2, C3> @this, params Tuple<T1, T2, T3>[] values)
            where C1 : IDbExpression<T1>
            where C2 : IDbExpression<T2>
            where C3 : IDbExpression<T3>
        {
            return new SqlBoolean(sql =>
            {
                if (values.Length == 0)
                {
                    sql.Append("1=2");
                    return;
                }

                sql.Append('(')
                    .Append(@this.Item1).Append(',')
                    .Append(@this.Item2).Append(',')
                    .Append(@this.Item3)
                .Append(") IN (");

                string delimiter = string.Empty;
                foreach (var t in values)
                {
                    sql.Append(delimiter).Append('(');

                    if (t.Item1 == null) sql.Append("NULL");
                    else sql.AppendParam(@this.Item1.DbType, t.Item1);

                    sql.Append(',');

                    if (t.Item2 == null) sql.Append("NULL");
                    else sql.AppendParam(@this.Item2.DbType, t.Item2);

                    sql.Append(',');

                    if (t.Item3 == null) sql.Append("NULL");
                    else sql.AppendParam(@this.Item3.DbType, t.Item3);

                    sql.Append(')');
                    delimiter = ", ";
                }
                sql.Append(')');
            });
        }

        public static SqlBoolean In<C1, C2, C3, T1, T2, T3>(this Tuple<C1, C2, C3> @this, IEnumerable<Tuple<T1, T2, T3>> values)
            where C1 : IDbExpression<T1>
            where C2 : IDbExpression<T2>
            where C3 : IDbExpression<T3>
        {
            return @this.In(values.ToArray());
        }

        public static SqlBoolean In<C1, C2, C3, T1, T2, T3>(this Tuple<C1, C2, C3> @this, IQueryContext<Tuple<T1, T2, T3>> subquery)
            where C1 : IDbExpression<T1>
            where C2 : IDbExpression<T2>
            where C3 : IDbExpression<T3>
        {
            return new SqlBoolean(sql =>
                sql.Append('(')
                    .Append(@this.Item1).Append(',')
                    .Append(@this.Item2).Append(',')
                    .Append(@this.Item3).Append(") IN (")
                    .Append(subquery)
                    .Append(')'));
        }
        #endregion

        #region Delete
        public static int Delete<C, T>(this QueryContext<C, T, T> @this, Func<C, SqlBoolean> predicate = null)
            where T : DbEntity
            where C : class,new()
        {
            if (predicate != null)
                @this = @this.Where(predicate);

            using (var cmd = @this.Db.CreateCommand())
            {
                var sql = new SqlContext(new StringBuilder(32), cmd.Parameters);
                sql.Append("DELETE FROM (").Append("SELECT ").Append(sql.GetAlias(@this) + ".*", @this).Append(')');
                cmd.CommandText = sql.ToString();
                return @this.Db.ExecuteNonQuery(cmd);
            }
        }
        #endregion
    }
}
