using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Xml.Serialization;
using Oracle.ManagedDataAccess.Client;
using System.Runtime.Serialization;
using System.Runtime.CompilerServices;
namespace Linq2Oracle {

    interface IDbMetaInfo {
        OracleDbType DbType { get; }
        int Size { get; }
    }

    sealed class DbExpressionMetaInfo : IDbMetaInfo {
        public OracleDbType DbType { get; set; }
        public int Size { get; set; }
    }

    [DebuggerDisplay("{ColumnName,nq},{DbType}({Size})")]
    sealed class DbColumn : IDbMetaInfo {
        public DbColumn(PropertyInfo property, DbColumn c) : this(c.TableName, c.ColumnName, c.ColumnIndex, property, c._attr) { }
        public DbColumn(string tableName, string columnName, int columnIndex, PropertyInfo property, ColumnAttribute attr) {
            TableName = tableName;
            ColumnName = columnName;
            QuotesColumnName = "\"" + columnName + "\"";
            TableQuotesColumnName = tableName + "." + QuotesColumnName;
            ColumnIndex = columnIndex;
            _attr = attr;
            PropertyInfo = property;
            _propGetter = (Func<object, object>)propGetterMaker.MakeGenericMethod(property.ReflectedType, property.PropertyType).Invoke(null, new object[] { property });
        }
        internal readonly PropertyInfo PropertyInfo;
        readonly Func<object, object> _propGetter;
        readonly ColumnAttribute _attr;
        public readonly string TableName;
        public readonly string ColumnName;
        public readonly string QuotesColumnName;
        public readonly string TableQuotesColumnName;
        public readonly int ColumnIndex;
        public bool IsPrimarykey { get { return _attr.IsPrimarykey; } }
        public bool IsNullable { get { return _attr.IsNullable; } }
        public OracleDbType DbType { get { return _attr.DbType; } }
        public int Size { get { return _attr.Size; } }
        /// <summary>
        /// Get Column Value.
        /// </summary>
        /// <param name="this">entity object</param>
        /// <returns>return DBNull if column value is null</returns>
        public object GetDbValue(object @this)
        {
            return _propGetter(@this);
        }

        static readonly MethodInfo propGetterMaker = typeof(DbColumn).GetMethod("GetPropertyGetter", BindingFlags.Static | BindingFlags.NonPublic);
        static Func<object, object> GetPropertyGetter<T, TProperty>(PropertyInfo pi) {
            var getter = (Func<T, TProperty>)Delegate.CreateDelegate(typeof(Func<T, TProperty>), pi.GetGetMethod());

            Type propertyType = typeof(TProperty);

            if (propertyType.IsEnum)
                return @this => Enum.GetName(propertyType, getter((T)@this));

            if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>)) {
                var nullT = propertyType.GetGenericArguments()[0];
                if (nullT.IsEnum)
                    return @this => {
                        var value = getter((T)@this);
                        return value != null ? Enum.GetName(nullT, value) : (object)DBNull.Value;
                    };
            }

            return @this => (object)getter((T)@this) ?? DBNull.Value;
        }
    }

    [Serializable]
    public abstract class DbEntity : INotifyPropertyChanged
    {
        [XmlIgnore]
        public bool IsLoaded { get; internal set; }

        public bool IsChanged { get { return ChangedMap.Count > 0; } }

        /// <summary>
        /// key: ColumnIndex, value: originalValue(db value)
        /// </summary>
        [NonSerialized]
        SortedList<int, object> _changedMap = _EmptyChangeMap;//有欄位變更才初始化

        [OnDeserialized]
        void Init(StreamingContext context)
        {
            _changedMap = _EmptyChangeMap;
        }

        static readonly SortedList<int, object> _EmptyChangeMap = new SortedList<int,object>(0);

        internal SortedList<int, object> ChangedMap { get { return _changedMap; } }

        protected void BeforeColumnChange([CallerMemberNameAttribute]string columnName="")
        {
            if (!IsLoaded)
                return;

            var tableInfo = Table.GetTableInfo(this.GetType());

            DbColumn c;
            if (!tableInfo.DbColumnMap.TryGetValue(columnName, out c))
                return;

            if (_changedMap == _EmptyChangeMap)
                _changedMap = new SortedList<int, object>();

            if (!_changedMap.ContainsKey(c.ColumnIndex))
            {
                _changedMap.Add(c.ColumnIndex, c.GetDbValue(this));
                NotifyPropertyChanged("IsChanged");
            }
        }

        internal protected virtual void OnSaving() { }

        #region INotifyPropertyChanged 成員

        public event PropertyChangedEventHandler PropertyChanged;

        protected void NotifyPropertyChanged([CallerMemberNameAttribute]string propertyName="")
        {
            if (PropertyChanged != null)
                PropertyChanged(this, new PropertyChangedEventArgs(propertyName));
        }  
        #endregion
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
                    .Select(fix => DbColumnMap[fix.ColumnName]).Where(c => !c.IsPrimarykey).ToArray();

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
                for (int k = 0, cnt = PkColumns.Length; k < cnt; k++)
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
                for (int k = 0, cnt = PkColumns.Length; k < cnt; k++)
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

                for (int k = 0, cnt = PkColumns.Length; k < cnt; k++)
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
        static internal Info GetTableInfo(Type entityType)
        {
            return _cache.GetOrAdd(entityType, t => new Info(t));
        }
    }

    static class Table<T> where T : DbEntity
    {
        internal static string TableName { get { return Info.TableName; } }
        internal static DbColumn[] DbColumns { get { return Info.DbColumns; } }
        internal static Dictionary<string, DbColumn> DbColumnMap { get { return Info.DbColumnMap; } }
        internal static DbColumn[] PkColumns { get { return Info.PkColumns; } }
        internal static DbColumn[] NonPkColumns { get { return Info.NonPkColumns; } }
        internal static DbColumn[] FixedColumns { get { return Info.FixedColumns; } }
        internal static string InsertSql { get { return Info.InsertSql; } }
        internal static string FullUpdateSql { get { return Info.FullUpdateSql; } }
        internal static string FullSelectionColumnsString { get { return Info.FullSelectionColumnsString; } }
        internal static string DeleteWithPK { get { return Info.DeleteWithPK; } }
        internal static string InsertOrUpdateSql { get { return Info.InsertOrUpdateSql; } }

        internal static readonly Table.Info Info = Table.GetTableInfo(typeof(T));
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

        static TableReader() {
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
                    bindings: from c in Table<T>.Info.DbColumns
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