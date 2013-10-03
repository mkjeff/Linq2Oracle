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
        public static SqlBoolean In<E, T>(this E @this, IEnumerable<T> values)
            where E : IDbExpression<T>
        {
            return @this.In(values.ToArray());
        }

        public static SqlBoolean In<E, T>(this E @this, T[] values)
            where E : IDbExpression<T>
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

                bool isEnum = typeof(T).IsEnum;
                string delimiter = string.Empty;
                foreach (var t in values)
                {
                    sql.Append(delimiter);

                    if (t == null)
                        sql.Append("NULL");
                    else if (isEnum)
                        sql.AppendParam(OracleDbType.Varchar2, t);
                    else
                        sql.AppendParam(t);
                    delimiter = ", ";
                }

                sql.Append(')');
            });
        }

        public static SqlBoolean In<E, T>(this E @this, IQueryContext<T> subquery)
            where E : IDbExpression<T>
        {
            return new SqlBoolean(sql => sql.Append(@this).Append(" IN (").AppendQuery(subquery).Append(')'));
        }
        #endregion
        #region Where (Column1,Column2) In (...)
        public static SqlBoolean In<E1, E2, T1, T2>(this Tuple<E1, E2> @this, Tuple<T1, T2>[] values)
            where E1 : IDbExpression<T1>
            where E2 : IDbExpression<T2>
        {
            return new SqlBoolean(sql =>
            {
                if (values.Length == 0)
                {
                    sql.Append("1=2");
                    return;
                }

                sql.Append('(').Append(@this.Item1).Append(',').Append(@this.Item2).Append(") IN (");

                bool t1IsEnum = typeof(T1).IsEnum;
                bool t2IsEnum = typeof(T2).IsEnum;
                string delimiter = string.Empty;
                foreach (var t in values)
                {
                    sql.Append(delimiter).Append('(');

                    if (t.Item1 == null) sql.Append("NULL");
                    else if (t1IsEnum) sql.AppendParam(OracleDbType.Varchar2, t.Item1);
                    else sql.AppendParam(t.Item1);

                    sql.Append(',');

                    if (t.Item2 == null) sql.Append("NULL");
                    else if (t2IsEnum) sql.AppendParam(OracleDbType.Varchar2, t.Item2);
                    else sql.AppendParam(t.Item2);

                    sql.Append(')');

                    delimiter = ", ";
                }

                sql.Append(")");
            });
        }

        public static SqlBoolean In<E1, E2, T1, T2>(this Tuple<E1, E2> @this, IEnumerable<Tuple<T1, T2>> values)
            where E1 : IDbExpression<T1>
            where E2 : IDbExpression<T2>
        {
            return @this.In(values.ToArray());
        }

        public static SqlBoolean In<E1, E2, T1, T2>(this Tuple<E1, E2> @this, IQueryContext<Tuple<T1, T2>> subquery)
            where E1 : IDbExpression<T1>
            where E2 : IDbExpression<T2>
        {
            return new SqlBoolean(sql =>
                sql.Append('(')
                    .Append(@this.Item1).Append(',')
                    .Append(@this.Item2).Append(") IN (")
                    .AppendQuery(subquery)
                    .Append(')'));
        }
        #endregion
        #region Where (Column1,Column2,Column3) In (...)
        public static SqlBoolean In<E1, E2, E3, T1, T2, T3>(this Tuple<E1, E2, E3> @this, Tuple<T1, T2, T3>[] values)
            where E1 : IDbExpression<T1>
            where E2 : IDbExpression<T2>
            where E3 : IDbExpression<T3>
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

                bool t1IsEnum = typeof(T1).IsEnum;
                bool t2IsEnum = typeof(T2).IsEnum;
                bool t3IsEnum = typeof(T3).IsEnum;
                string delimiter = string.Empty;
                foreach (var t in values)
                {
                    sql.Append(delimiter).Append('(');

                    if (t.Item1 == null) sql.Append("NULL");
                    else if (t1IsEnum) sql.AppendParam(OracleDbType.Varchar2, t.Item1);
                    else sql.AppendParam(t.Item1);

                    sql.Append(',');

                    if (t.Item2 == null) sql.Append("NULL");
                    else if (t2IsEnum) sql.AppendParam(OracleDbType.Varchar2, t.Item2);
                    else sql.AppendParam(t.Item2);

                    sql.Append(',');

                    if (t.Item3 == null) sql.Append("NULL");
                    else if (t3IsEnum) sql.AppendParam(OracleDbType.Varchar2, t.Item3);
                    else sql.AppendParam(t.Item3);

                    sql.Append(')');
                    delimiter = ", ";
                }
                sql.Append(')');
            });
        }

        public static SqlBoolean In<E1, E2, E3, T1, T2, T3>(this Tuple<E1, E2, E3> @this, IEnumerable<Tuple<T1, T2, T3>> values)
            where E1 : IDbExpression<T1>
            where E2 : IDbExpression<T2>
            where E3 : IDbExpression<T3>
        {
            return @this.In(values.ToArray());
        }

        public static SqlBoolean In<E1, E2, E3, T1, T2, T3>(this Tuple<E1, E2, E3> @this, IQueryContext<Tuple<T1, T2, T3>> subquery)
            where E1 : IDbExpression<T1>
            where E2 : IDbExpression<T2>
            where E3 : IDbExpression<T3>
        {
            return new SqlBoolean(sql =>
                sql.Append('(')
                    .Append(@this.Item1).Append(',')
                    .Append(@this.Item2).Append(',')
                    .Append(@this.Item3).Append(") IN (")
                    .AppendQuery(subquery)
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
