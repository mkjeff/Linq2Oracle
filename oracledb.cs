using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;

namespace Linq2Oracle
{
    /// <summary>
    /// Database Access Class
    /// </summary>
    public class OracleDB
    {
        readonly OracleConnection _conn;
        TextWriter _logger;
        public TextWriter Log { get { return _logger; } set { _logger = value; } }

        /// <summary>
        /// Create Database Access Object
        /// </summary>
        /// <param name="connection">Database connection</param>
        /// <param name="logger">log writer</param>
        public OracleDB(OracleConnection connection, TextWriter logger = null)
        {
            _conn = connection;
            if (logger == TextWriter.Null)
                logger = null;
            _logger = logger;
        }

        #region Private Methods
        OracleCommand PrepareDbCommand(OracleCommand cmd)
        {
            if (_conn.State != ConnectionState.Open)
            {
                _conn.Open();
                //_trx = _connection.BeginTransaction();
            }
            //cmd.Transaction = _trx;
            cmd.Connection = _conn;

            if (_logger != null)
            {
                #region Logging SQL TEXT
                _logger.Write(cmd.CommandText);
                _logger.WriteLine(";");

                for (int i = 0, len = cmd.Parameters.Count; i < len; i++)
                {
                    var p = cmd.Parameters[i];
                    _logger.Write("\t--");
                    _logger.Write(p.ParameterName);
                    _logger.Write(" = ");
                    if (p.Value != null)
                    {
                        _logger.Write(p.OracleDbType);
                        _logger.Write('[');
                        if (p.Value is byte[])
                        {
                            _logger.Write("Binary Data");
                        }
                        else if (p.Value is Array)
                        {
                            var values = (object[])p.Value;
                            var count = values.Length;
                            for (int v = 0; v < count - 1; v++)
                            {
                                _logger.Write(values[v]);
                                _logger.Write(", ");
                            }
                            _logger.Write(values[count - 1]);
                        }
                        else
                            _logger.Write(p.Value);
                        _logger.Write("]");
                    }
                    _logger.WriteLine();
                }
                #endregion
            }
            return cmd;
        }

        object CheckPkNull(string columnName, object value)
        {
            if (value == null)
                throw new DalException(DbErrorCode.E_DB_PK_NULL, $"主鍵 {columnName} 不能為NULL");
            return value;
        }
        #endregion
        #region Command Free Methods
        public OracleCommand CreateCommand() => _conn.CreateCommand();

        public object ExecuteScalar(OracleCommand cmd) => PrepareDbCommand(cmd).ExecuteScalar();

        public int ExecuteNonQuery(OracleCommand cmd) => PrepareDbCommand(cmd).ExecuteNonQuery();

        public OracleDataReader ExecuteReader(OracleCommand cmd) => PrepareDbCommand(cmd).ExecuteReader();
        #endregion
        #region Batch Query
        public void BatchQuery(params IQueryContext[] querys)
        {
            using (var cmd = this.CreateCommand())
            {
                var sql = new SqlContext(new StringBuilder("BEGIN\n", 512 * querys.Length), cmd.Parameters);
                bool valid = false;
                foreach (var q in querys)
                    if (q.Db == this)
                    {
                        var refParam = cmd.Parameters.Add(cmd.Parameters.Count.ToString(), OracleDbType.RefCursor, ParameterDirection.Output);
                        sql.Append("OPEN :")
                          .Append(refParam.ParameterName)
                          .Append(" FOR ");
                        q.GenBatchSql(sql, refParam);
                        sql.Append(";\n");
                        valid = true;
                    }
                if (!valid) return;
                sql.Append("END;");
                cmd.CommandText = sql.ToString();
                ExecuteNonQuery(cmd);
            }
        }
        #endregion
        #region Insert
        /// <summary>
        /// Insert new record
        /// </summary>
        /// <typeparam name="T">Entity type</typeparam>
        /// <param name="t">entity object</param>
        public T Insert<T>(T t) where T : DbEntity
        {
            t.OnSaving();
            using (var cmd = new OracleCommand(Table<T>.InsertSql, _conn))
            {
                foreach (var c in Table<T>.DbColumns)
                    cmd.Parameters.Add(
                        cmd.Parameters.Count.ToString(),
                        c.DbType,
                        c.Size,
                        c.GetValue(t),
                        ParameterDirection.Input);

                if (ExecuteNonQuery(cmd) != 1)
                    throw new DalException(DbErrorCode.E_DB_INSERT_FAIL, "資料庫新增紀錄失敗");
            }
            t.ChangedMap.Clear();
            t.IsLoaded = true;
            return t;
        }

        /// <summary>
        /// Batch Insert
        /// </summary>
        /// <typeparam name="T">Entity type</typeparam>
        /// <param name="list">insert list</param>
        public T[] Insert<T>(params T[] list) where T : DbEntity
        {
            if (list.Length == 0)
                return list;

            foreach (var t in list)
            {
                t.OnSaving();
                t.IsLoaded = true;
                t.ChangedMap.Clear();
            }

            using (var cmd = new OracleCommand(Table<T>.InsertSql, _conn))
            {
                cmd.ArrayBindCount = list.Length;
                foreach (var column in Table<T>.DbColumns)
                    cmd.Parameters.Add(
                        cmd.Parameters.Count.ToString(),
                        column.DbType,
                        column.Size,
                        list.ConvertAll(t => column.GetValue(t)),
                        ParameterDirection.Input);

                ExecuteNonQuery(cmd);
            }
            return list;
        }

        /// <summary>
        /// Batch Insert
        /// </summary>
        /// <typeparam name="T">Entity type</typeparam>
        /// <param name="list">insert list</param>
        public IEnumerable<T> Insert<T>(IEnumerable<T> list) where T : DbEntity => Insert(list.ToArray());
        #endregion
        #region Delete
        public bool Delete<T>(T t) where T : DbEntity
        {
            if (Table<T>.PkColumns.Length == 0)
                throw new DalException(DbErrorCode.E_DB_SQL_INVAILD, Table<T>.TableName + "沒有主鍵, 必須使用另一個多載方法提供Where條件");

            using (var cmd = this.CreateCommand())
            {
                cmd.CommandText = Table<T>.DeleteWithPK;
                foreach (var c in Table<T>.PkColumns)
                    cmd.Parameters.Add(
                        cmd.Parameters.Count.ToString(),
                        c.DbType,
                        c.Size,
                        CheckPkNull(c.ColumnName, c.GetValue(t)),
                        ParameterDirection.Input);

                t.IsLoaded = false;
                t.ChangedMap.Clear();
                if (ExecuteNonQuery(cmd) == 1)
                    return true;
            }
            return false;
        }

        /// <summary>
        /// 刪除多筆紀錄，用於刪除已經讀取的紀錄
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <returns></returns>
        public int Delete<T>(IEnumerable<T> list) where T : DbEntity => Delete<T>(list.ToArray());

        /// <summary>
        /// 刪除多筆紀錄，用於刪除已經讀取的紀錄
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <returns></returns>
        public int Delete<T>(params T[] list) where T : DbEntity
        {
            if (Table<T>.PkColumns.Length == 0)
                throw new DalException(DbErrorCode.E_DB_SQL_INVAILD, Table<T>.TableName + "沒有主鍵, 必須使用另一個多載方法提供Where條件");

            if (list.Length == 0)
                return 0;

            foreach (var t in list)
            {
                t.IsLoaded = false;
                t.ChangedMap.Clear();
            }

            using (var cmd = this.CreateCommand())
            {
                cmd.ArrayBindCount = list.Length;
                cmd.CommandText = Table<T>.DeleteWithPK;
                foreach (var c in Table<T>.PkColumns)
                    cmd.Parameters.Add(
                        cmd.Parameters.Count.ToString(),
                        c.DbType,
                        c.Size,
                        list.ConvertAll(t => CheckPkNull(c.ColumnName, c.GetValue(t))),
                        ParameterDirection.Input);

                return ExecuteNonQuery(cmd);
            }
        }
        #endregion
        #region Update
        /// <summary>
        /// 更新單筆紀錄
        /// </summary>
        /// <remarks>
        /// 更新模式有兩種
        /// 1. 已載入情況下的更新; 實體有經過查詢(Query)下的更新,這種情況下只有變更的欄位會更新
        /// 2. 未經載入情況下的更新; 這種情況下會對實體的所有欄位做更新
        /// 
        /// 在模式2的情況下,如果實體有主鍵,則不會使用樂觀同步欄位當做更新條件
        /// </remarks>
        /// <typeparam name="T">Entity type</typeparam>
        /// <param name="t">entity object</param>
        /// <returns>effect row count is 1 return true,otherwise false </returns>
        public bool Update<T>(T t) where T : DbEntity
        {
            if (Table<T>.PkColumns.Length == 0 && Table<T>.FixedColumns.Length == 0)
                throw new DalException(DbErrorCode.E_DB_SQL_INVAILD, Table<T>.TableName + "沒有主鍵且沒有樂觀同步欄位, 必須使用另一個多載方法提供Where條件");

            t.OnSaving();
            if (t.IsLoaded && !t.IsChanged)
                return true;//no column need update

            object value = null;//for column value
            using (var cmd = this.CreateCommand())
            {
                var sql = new StringBuilder();
                if (t.IsLoaded)
                {
                    #region update changed only
                    sql.Append("UPDATE ").AppendLine(Table<T>.TableName)
                       .Append("SET ");
                    for (int i = 0, cnt = t.ChangedMap.Keys.Count; i < cnt; i++)
                    {
                        if (i != 0) sql.Append(',');
                        var c = Table<T>.DbColumns[t.ChangedMap.Keys[i]];
                        sql.Append(c.QuotesColumnName).Append(" = ").AppendParam(cmd.Parameters, c.DbType, c.GetValue(t));
                    }

                    sql.AppendLine().Append("WHERE ");
                    //pk condition
                    for (int i = 0, cnt = Table<T>.PkColumns.Length; i < cnt; i++)
                    {
                        if (i != 0) sql.Append(" AND ");
                        var c = Table<T>.PkColumns[i];
                        sql.Append(c.QuotesColumnName).Append(" = ").AppendParam(cmd.Parameters, c.DbType, CheckPkNull(c.ColumnName, t.ChangedMap.TryGetValue(c.ColumnIndex, out value) ? value : c.GetValue(t)));
                    }

                    if (Table<T>.FixedColumns.Length != 0)
                    {
                        if (Table<T>.PkColumns.Length != 0)
                            sql.Append(" AND ");
                        // fixed column condition
                        for (int i = 0, cnt = Table<T>.FixedColumns.Length; i < cnt; i++)
                        {
                            if (i != 0) sql.Append(" AND ");
                            var c = Table<T>.FixedColumns[i];
                            sql.Append(c.QuotesColumnName);
                            if ((value = t.ChangedMap.TryGetValue(c.ColumnIndex, out value) ? value : c.GetValue(t)) == null)
                                sql.Append(" IS NULL");
                            else
                                sql.Append(" = ").AppendParam(cmd.Parameters, c.DbType, value);
                        }
                    }
                    #endregion
                }
                else
                {
                    #region full row update
                    sql.Append(Table<T>.FullUpdateSql);
                    foreach (var c in Table<T>.NonPkColumns)
                        cmd.Parameters.Add(
                            cmd.Parameters.Count.ToString(),
                            c.DbType,
                            c.Size,
                            c.GetValue(t),
                            ParameterDirection.Input);

                    foreach (var c in Table<T>.PkColumns)
                        cmd.Parameters.Add(
                            cmd.Parameters.Count.ToString(),
                            c.DbType,
                            c.Size,
                            CheckPkNull(c.ColumnName, c.GetValue(t)),
                            ParameterDirection.Input);

                    if (Table<T>.PkColumns.Length == 0)
                    {
                        //沒有主鍵才會用到樂觀同步欄位
                        for (int i = 0, cnt = Table<T>.FixedColumns.Length; i < cnt; i++)
                        {
                            if (i != 0) sql.Append(" AND ");
                            var c = Table<T>.FixedColumns[i];
                            sql.Append(c.QuotesColumnName);
                            if ((value = c.GetValue(t)) == null)
                                sql.Append(" IS NULL");
                            else
                                sql.Append(" = ").AppendParam(cmd.Parameters, c.DbType, value);
                        }
                    }
                    #endregion
                }

                cmd.CommandText = sql.ToString();
                if (ExecuteNonQuery(cmd) == 1)
                {
                    t.IsLoaded = true;
                    t.ChangedMap.Clear();
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// 更新多筆紀錄
        /// </summary>
        /// <typeparam name="T">Entity type</typeparam>
        /// <param name="list">update list</param>
        /// <returns>effect row count</returns>
        public int Update<T>(IEnumerable<T> list) where T : DbEntity => Update<T>(list.ToArray());

        /// <summary>
        /// 批次更新
        /// </summary>
        /// <remarks>
        /// 更新模式有兩種
        /// 1. 已載入情況下的更新; 實體有經過查詢(Query)下的更新,這種情況下只有變更的欄位會更新
        /// 2. 未經載入情況下的更新; 這種情況下會對實體的所有欄位做更新
        ///
        /// 在模式2的情況下,如果實體有主鍵,則不會使用樂觀同步欄位當做更新條件;
        /// 如果沒有主鍵則必須所有的樂觀同步欄位值都不是NULL才能批次更新
        /// </remarks>
        /// <typeparam name="T">Entity type</typeparam>
        /// <param name="list">update list</param>
        /// <returns>effect row count</returns>
        public int Update<T>(params T[] list) where T : DbEntity
        {
            if (Table<T>.PkColumns.Length == 0 && Table<T>.FixedColumns.Length == 0)
                throw new DalException(DbErrorCode.E_DB_SQL_INVAILD, Table<T>.TableName + "沒有主鍵且沒有樂觀同步欄位, 必須使用另一個多載方法提供Where條件");

            Action<T> updatedComplete = t =>
            {
                t.IsLoaded = true;
                t.ChangedMap.Clear();
            };
            return list.ToLookup(t => t.IsLoaded).Sum(group =>
            {
                if (group.Key)
                {
                    #region Changed only update
                    return group.GroupBy(e => e.ChangedMap, (changedColumns, g) =>
                    {
                        int count = g.Count();
                        if (changedColumns.Count == 0) return count; // loaded ,but no change
                        if (count == 1) return Update(g.First()) ? 1 : 0; // single row update command 

                        using (var cmd = this.CreateCommand())
                        {
                            cmd.ArrayBindCount = count;

                            object value = null; // for changed value

                            var sql = new StringBuilder("UPDATE ").AppendLine(Table<T>.TableName)
                                .Append("SET ");
                            for (int i = 0, cnt = changedColumns.Keys.Count; i < cnt; i++)
                            {
                                if (i != 0) sql.Append(',');
                                var c = Table<T>.DbColumns[changedColumns.Keys[i]];
                                sql.Append(c.QuotesColumnName).Append(" = ").AppendParam(cmd.Parameters, c.DbType, g.Select(t => c.GetValue(t)).ToArray());
                            }
                            sql.AppendLine().Append("WHERE ");

                            for (int i = 0, cnt = Table<T>.PkColumns.Length; i < cnt; i++)
                            {
                                if (i != 0) sql.Append(" AND ");
                                var c = Table<T>.PkColumns[i];
                                sql.Append(c.QuotesColumnName).Append(" = ").AppendParam(cmd.Parameters, c.DbType,
                                    g.Select(t => CheckPkNull(c.ColumnName, t.ChangedMap.TryGetValue(c.ColumnIndex, out value) ? value : c.GetValue(t))).ToArray());
                            }

                            if (Table<T>.FixedColumns.Length != 0)
                            {
                                if (Table<T>.PkColumns.Length != 0)
                                    sql.Append(" AND ");
                                for (int i = 0, cnt = Table<T>.FixedColumns.Length; i < cnt; i++)
                                {
                                    if (i != 0) sql.Append(" AND ");
                                    var c = Table<T>.FixedColumns[i];
                                    if ((g.First().ChangedMap.TryGetValue(c.ColumnIndex, out value) ? value : c.GetValue(g.First())) == null)
                                        sql.Append(c.QuotesColumnName).Append(" IS NULL");
                                    else
                                        sql.Append(c.QuotesColumnName).Append(" = ").AppendParam(cmd.Parameters, c.DbType,
                                            g.Select(t => t.ChangedMap.TryGetValue(c.ColumnIndex, out value) ? value : c.GetValue(t)).ToArray());
                                }
                            }

                            cmd.CommandText = sql.ToString();
                            int effectCount = ExecuteNonQuery(cmd);
                            if (effectCount == cmd.ArrayBindCount)
                                g.ForEach(updatedComplete);

                            return effectCount;
                        }
                    }, ChangedGroupComparer<T>.Instance).Sum();
                    #endregion
                }
                else
                {
                    #region Full row update
                    int count = group.Count();
                    if (count == 1) return Update(group.First()) ? 1 : 0; // single row update command

                    using (var cmd = this.CreateCommand())
                    {
                        cmd.ArrayBindCount = count;
                        var sql = new StringBuilder(Table<T>.FullUpdateSql);

                        foreach (var c in Table<T>.NonPkColumns)
                            cmd.Parameters.Add(
                                cmd.Parameters.Count.ToString(),
                                c.DbType,
                                c.Size,
                                group.Select(t => c.GetValue(t)).ToArray(),
                                ParameterDirection.Input);

                        foreach (var c in Table<T>.PkColumns)
                            cmd.Parameters.Add(
                                cmd.Parameters.Count.ToString(),
                                c.DbType,
                                c.Size,
                                group.Select(t => CheckPkNull(c.ColumnName, c.GetValue(t))).ToArray(),
                                ParameterDirection.Input);

                        if (Table<T>.PkColumns.Length == 0)
                            for (int i = 0, cnt = Table<T>.FixedColumns.Length; i < cnt; i++)
                            {
                                if (i != 0) sql.Append(" AND ");
                                var c = Table<T>.FixedColumns[i];
                                sql.Append(c.QuotesColumnName).Append(" = ").AppendParam(cmd.Parameters, c.DbType, group.Select(t =>
                                {
                                    object value = c.GetValue(t);
                                    if (value == null) throw new DalException(DbErrorCode.E_DB_NOT_SUPPORT_OPERATOR, "無載入模式下的批次更新作業且沒有主鍵情況下而使用樂觀同步欄位當做更新條件時,欄位值不能為NULL");
                                    return value;
                                }).ToArray());
                            }

                        cmd.CommandText = sql.ToString();
                        var effectCount = ExecuteNonQuery(cmd);
                        if (effectCount == cmd.ArrayBindCount)
                            group.ForEach(updatedComplete);

                        return effectCount;
                    }
                    #endregion
                }
            });
        }

        /// <summary>
        /// 用來將集合(T)依據變更欄分群(GroupBy)使得各群組可以執行批次更新作業,
        /// 如果變更欄位相同但是Fixed Column舊值為DBNull, 則必須視為不同群組,因為會影響WHERE條件
        /// </summary>
        /// <typeparam name="T"></typeparam>
        sealed class ChangedGroupComparer<T> : IEqualityComparer<SortedList<int, object>> where T : DbEntity
        {
            static readonly HashSet<int> NullableFixedColumns = new HashSet<int>(from c in Table<T>.FixedColumns where c.IsNullable select c.ColumnIndex);
            internal static readonly ChangedGroupComparer<T> Instance = new ChangedGroupComparer<T>();
            ChangedGroupComparer() { }

            #region IEqualityComparer<SortedList<string,object>> 成員
            public bool Equals(SortedList<int, object> changedX, SortedList<int, object> changedY)
            {
                if (changedX == changedY)
                    return true;

                if (changedX.Count != changedY.Count)
                    return false;

                using (var Xs = changedX.GetEnumerator())
                using (var Ys = changedY.GetEnumerator())
                {
                    while (Xs.MoveNext() && Ys.MoveNext())
                    {
                        var x = Xs.Current;
                        var y = Ys.Current;
                        if (x.Key != y.Key)
                            return false;
                        if (NullableFixedColumns.Contains(x.Key) && (x.Value == null) != (y.Value == null))
                            return false;
                    }
                }
                return true;
            }

            /// <summary>
            /// GroupBy 會先依據GetHashCode作粗略的分割,再呼叫Equals作細部的比對
            /// Tip:如果雜湊函數夠準確,Equals可以忽略(always return true)
            /// </summary>
            /// <param name="obj"></param>
            /// <returns></returns>
            public int GetHashCode(SortedList<int, object> obj)
            {
                //unchecked {
                //    int hash = 23;
                //    foreach (var e in obj) {
                //        hash = hash * 37 + e.Key
                //            + (NullableFixedColumns.Contains(e.Key) && e.Value == DBNull.Value ? 1 : 0);
                //    }
                //    return hash;
                //}
                return obj.Count;
            }
            #endregion
        }
        #endregion
        #region InsertOrUpdate
        /// <summary>
        /// 只有未經讀取(Query)的資料才可以使用InsertOrUpdate，否則必須明確使用Update方法來更新
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="t"></param>
        /// <returns></returns>
        public T InsertOrUpdate<T>(T t) where T : DbEntity
        {
            if (Table<T>.PkColumns.Length == 0)
                throw new DalException(DbErrorCode.E_DB_NOT_SUPPORT_OPERATOR, "必需要有Primary Key才能執行InsertOrUpdate");

            if (t.IsLoaded)
                throw new DalException(DbErrorCode.E_DB_NOT_SUPPORT_OPERATOR, "只有未經讀取(Query)的資料才可以使用InsertOrUpdate，否則必須明確使用Update方法來更新");

            t.OnSaving();

            using (var cmd = new OracleCommand(Table<T>.InsertOrUpdateSql, _conn))
            {
                #region Condition
                foreach (var c in Table<T>.PkColumns)
                    cmd.Parameters.Add(
                        cmd.Parameters.Count.ToString(),
                        c.DbType,
                        c.Size,
                        c.GetValue(t),
                        ParameterDirection.Input);
                #endregion
                #region Update
                foreach (var c in Table<T>.NonPkColumns)
                    cmd.Parameters.Add(
                        cmd.Parameters.Count.ToString(),
                        c.DbType,
                        c.Size,
                        c.GetValue(t),
                        ParameterDirection.Input);
                #endregion
                #region Insert
                foreach (var c in Table<T>.DbColumns)
                    cmd.Parameters.Add(
                        cmd.Parameters.Count.ToString(),
                        c.DbType,
                        c.Size,
                        c.GetValue(t),
                        ParameterDirection.Input);
                #endregion
                if (ExecuteNonQuery(cmd) != -1)
                    throw new DalException(DbErrorCode.E_DB_EXEC_PROCEDURE_FAIL, "InsertOrUpdate Failed");
            }
            t.ChangedMap.Clear();
            t.IsLoaded = true;
            return t;
        }

        /// <summary>
        /// Batch Insert/Update
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <returns></returns>
        public IEnumerable<T> InsertOrUpdate<T>(IEnumerable<T> list) where T : DbEntity => InsertOrUpdate(list.ToArray());

        /// <summary>
        /// Batch Insert/Update
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <returns></returns>
        public T[] InsertOrUpdate<T>(params T[] list) where T : DbEntity
        {
            if (list.Length == 0)
                return list;

            if (Table<T>.PkColumns.Length == 0)
                throw new DalException(DbErrorCode.E_DB_NOT_SUPPORT_OPERATOR, "必需要有Primary Key才能執行InsertOrUpdate");

            if (list.Any(t => t.IsLoaded))
                throw new DalException(DbErrorCode.E_DB_NOT_SUPPORT_OPERATOR, "只有未經讀取(Query)的資料才可以使用InsertOrUpdate，否則必須明確使用Update方法來更新");

            foreach (var t in list)
                t.OnSaving();

            using (var cmd = new OracleCommand(Table<T>.InsertOrUpdateSql, _conn))
            {
                cmd.ArrayBindCount = list.Length;
                #region Condition
                foreach (var c in Table<T>.PkColumns)
                    cmd.Parameters.Add(
                        cmd.Parameters.Count.ToString(),
                        c.DbType,
                        c.Size,
                        list.ConvertAll(t => c.GetValue(t)),
                        ParameterDirection.Input);
                #endregion
                #region Update
                foreach (var c in Table<T>.NonPkColumns)
                    cmd.Parameters.Add(
                        cmd.Parameters.Count.ToString(),
                        c.DbType,
                        c.Size,
                        list.ConvertAll(t => c.GetValue(t)),
                        ParameterDirection.Input);
                #endregion
                #region Insert
                foreach (var c in Table<T>.DbColumns)
                    cmd.Parameters.Add(
                        cmd.Parameters.Count.ToString(),
                        c.DbType,
                        c.Size,
                        list.ConvertAll(t => c.GetValue(t)),
                        ParameterDirection.Input);
                #endregion
                if (ExecuteNonQuery(cmd) != -1)
                    throw new DalException(DbErrorCode.E_DB_EXEC_PROCEDURE_FAIL, "InsertOrUpdate Failed");
            }
            foreach (var t in list)
            {
                t.ChangedMap.Clear();
                t.IsLoaded = true;
            }
            return list;
        }
        #endregion
    }
}
