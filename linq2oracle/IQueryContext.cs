using Oracle.ManagedDataAccess.Client;
using System.Collections.Generic;

namespace Linq2Oracle
{
    public interface IQueryContext
    {
        IQueryContext OriginalSource { get; }
        void GenInnerSql(SqlContext sql, string selection = null);
        void GenBatchSql(SqlContext sql, OracleParameter refParam);
        OracleDB Db { get; }
        string TableName { get; }
    }

    public interface IQueryContext<T> : IQueryContext, IEnumerable<T>
    {
        //IEnumerable<T> AsEnumerable();
    }
}
