using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Oracle.ManagedDataAccess.Client;
namespace Linq2Oracle
{
    [DebuggerDisplay("{ColumnName,nq},{DbType}({Size})")]
    sealed class DbColumn
    {
        public DbColumn(PropertyInfo property, DbColumn c) : this(c.TableName, c.ColumnName, c.ColumnIndex, property, c._attr) { }
        public DbColumn(string tableName, string columnName, int columnIndex, PropertyInfo property, ColumnAttribute attr)
        {
            TableName = tableName;
            ColumnName = columnName;
            QuotesColumnName = "\"" + columnName + "\"";
            TableQuotesColumnName = tableName + "." + QuotesColumnName;
            ColumnIndex = columnIndex;
            _attr = attr;
            PropertyInfo = property;
            _propGetter = (Func<object, object>)propGetterMaker.MakeGenericMethod(property.DeclaringType, property.PropertyType).Invoke(null, new object[] { property });
        }
        internal readonly PropertyInfo PropertyInfo;
        readonly Func<object, object> _propGetter;
        readonly ColumnAttribute _attr;
        public readonly string TableName;
        public readonly string ColumnName;
        public readonly string QuotesColumnName;
        public readonly string TableQuotesColumnName;
        public readonly int ColumnIndex;
        public bool IsPrimarykey => _attr.IsPrimarykey;
        public bool IsNullable => _attr.IsNullable;
        public OracleDbType DbType => _attr.DbType;
        public int Size => _attr.Size;
        /// <summary>
        /// Get Column Value.
        /// </summary>
        /// <param name="this">entity object</param>
        /// <returns>return DBNull if column value is null</returns>
        public object GetValue(object @this) => _propGetter(@this);

        static readonly MethodInfo propGetterMaker = typeof(DbColumn).GetMethod(nameof(DbColumn.GetPropertyGetter), BindingFlags.Static | BindingFlags.NonPublic);
        static Func<object, object> GetPropertyGetter<T, TProperty>(PropertyInfo pi)
        {
            var getter = (Func<T, TProperty>)Delegate.CreateDelegate(typeof(Func<T, TProperty>), pi.GetGetMethod());
            return @this => getter((T)@this);
        }
    }

    static class Table
    {
        #region Info class
        internal class Info
        {
            internal readonly string TableName;
            internal readonly DbColumn[] DbColumns;
            internal readonly Dictionary<string, DbColumn> DbColumnMap;
            internal readonly DbColumn[] PkColumns;
            internal readonly DbColumn[] NonPkColumns;
            internal readonly DbColumn[] FixedColumns;
            internal readonly string InsertSql;
            internal readonly string FullUpdateSql;
            internal readonly string FullSelectionColumnsString;
            internal readonly string DeleteWithPK;
            internal readonly string InsertOrUpdateSql;

            internal Info(Type t)
            {
                int i = 0;

                TableName = t.Name;

                DbColumns = (from p in t.GetProperties()
                             let attr = p.GetCustomAttribute<ColumnAttribute>()
                             where attr != null
                             orderby p.Name
                             select new DbColumn(TableName, p.Name, i++, p, attr)).ToArray();

                DbColumnMap = DbColumns.ToDictionary(c => c.ColumnName);

                PkColumns = (from c in DbColumns
                             where c.IsPrimarykey
                             select c).ToArray();

                NonPkColumns = (from c in DbColumns
                                where !c.IsPrimarykey
                                select c).ToArray();

                FixedColumns = t.GetCustomAttributes<ConcurrencyCheckAttribute>()
                    .Where(c => DbColumnMap.ContainsKey(c.ColumnName))
                    .Select(fix => DbColumnMap[fix.ColumnName])
                    .Where(c => !c.IsPrimarykey).ToArray();

                #region INSERT
                var sb = new StringBuilder(64);
                i = 0;
                InsertSql = sb.Append("INSERT INTO ").Append(TableName)
                    .Append('(').Append(string.Join(",", DbColumns.ConvertAll(c => c.QuotesColumnName))).Append(')').AppendLine()
                    .Append("VALUES(").Append(string.Join(",", DbColumns.ConvertAll(c => ":" + i++))).Append(')').ToString();
                #endregion
                #region UPDATE
                sb.Length = 0;
                i = 0;
                sb.Append("UPDATE ").AppendLine(TableName)
                  .Append("SET ").AppendLine(string.Join(",", NonPkColumns.ConvertAll(c => c.QuotesColumnName + " = :" + i++)))
                  .Append("WHERE");
                for (int k = 0; k < PkColumns.Length; k++)
                {
                    if (k != 0) sb.Append(" AND ");
                    var c = PkColumns[k];
                    sb.Append(c.QuotesColumnName).Append(" = :").Append(i++);
                }
                FullUpdateSql = sb.ToString();
                #endregion
                #region DELETE
                sb.Length = 0;
                sb.Append("DELETE FROM ").AppendLine(TableName)
                  .Append("WHERE ");
                for (int k = 0; k < PkColumns.Length; k++)
                {
                    if (k != 0) sb.Append(" AND ");
                    var c = PkColumns[k];
                    sb.Append(c.QuotesColumnName).Append(" = :").Append(k);
                }
                DeleteWithPK = sb.ToString();
                #endregion
                #region SELECT
                FullSelectionColumnsString = string.Join(",", DbColumns.ConvertAll(c => c.TableQuotesColumnName));
                #endregion
                #region InsertOrUpdate MERGE INTO
                i = 0;
                sb.Length = 0;
                sb.Append("MERGE INTO ").AppendLine(TableName)
                    .AppendLine("  USING (SELECT NULL FROM DUAL)")
                    .Append("  ON (");

                for (int k = 0; k < PkColumns.Length; k++)
                {
                    if (k != 0) sb.Append(" AND ");
                    var c = PkColumns[k];
                    sb.Append(c.QuotesColumnName).Append(" = :").Append(i++);
                }

                sb.AppendLine(")")
                    .AppendLine("WHEN MATCHED THEN ")
                    .Append("  UPDATE SET ").AppendLine(string.Join(",", NonPkColumns.ConvertAll(c => c.QuotesColumnName + " = :" + i++)));

                sb.AppendLine("WHEN NOT MATCHED THEN")
                    .Append("  INSERT (").Append(string.Join(",", DbColumns.ConvertAll(c => c.QuotesColumnName))).AppendLine(")")
                    .Append("  VALUES (").Append(string.Join(",", DbColumns.ConvertAll(c => ":" + i++))).Append(')');

                InsertOrUpdateSql = sb.ToString();
                #endregion
            }
        }
        #endregion

        static Table() { }
        static readonly ConcurrentDictionary<Type, Info> _cache = new ConcurrentDictionary<Type, Info>();
        static internal Info GetTableInfo(Type entityType) => _cache.GetOrAdd(entityType, t => new Info(t));
    }

    static class Table<T> where T : DbEntity
    {
        internal static string TableName => Info.TableName;
        internal static DbColumn[] DbColumns => Info.DbColumns;
        internal static Dictionary<string, DbColumn> DbColumnMap => Info.DbColumnMap;
        internal static DbColumn[] PkColumns => Info.PkColumns;
        internal static DbColumn[] NonPkColumns => Info.NonPkColumns;
        internal static DbColumn[] FixedColumns => Info.FixedColumns;
        internal static string InsertSql => Info.InsertSql;
        internal static string FullUpdateSql => Info.FullUpdateSql;
        internal static string FullSelectionColumnsString => Info.FullSelectionColumnsString;
        internal static string DeleteWithPK => Info.DeleteWithPK;
        internal static string InsertOrUpdateSql => Info.InsertOrUpdateSql;

        static readonly Table.Info Info = Table.GetTableInfo(typeof(T));
        static Table() { }
    }

    static class TableReader<T> where T : DbEntity
    {
        static readonly Func<OracleDataReader, T> readerFunc;

        internal static T Read(OracleDataReader reader)
        {
            var entity = readerFunc(reader);
            entity.IsLoaded = true;
            return entity;
        }

        static TableReader()
        {
            // (OracleReader reader)=>
            //  new T{ 
            //      PropertyOfColumn1 = getColumn1Value(reader,column1Index),
            //      PropertyOfColumn2 = getColumn2Value(reader,column2Index),
            //      ...
            //  }
            //
            var dbReader = Expression.Parameter(typeof(OracleDataReader), "reader");
            var expr = Expression.Lambda<Func<OracleDataReader, T>>(
                body: Expression.MemberInit(
                    newExpression: Expression.New(typeof(T)),
                    bindings: from c in Table<T>.DbColumns
                              select (MemberBinding)Expression.Bind(
                                 member: c.PropertyInfo,
                                 expression: Expression.Call(
                                       OracleDataReaderHelper.GetValueGetMethod(c.PropertyInfo.PropertyType, c.DbType, c.IsNullable),
                                       dbReader,
                                       Expression.Constant(c.ColumnIndex))
                    )
                ),
                parameters: dbReader);

            readerFunc = expr.Compile();
        }
    }
}