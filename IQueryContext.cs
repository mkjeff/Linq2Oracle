using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;
using System.Text;

namespace Linq2Oracle {
    public interface IQueryContext
    {
        void GenInnerSql(StringBuilder sql, OracleParameterCollection param);
        void GenInnerSql(StringBuilder sql, OracleParameterCollection param, string selection);
        void GenBatchSql(StringBuilder sql, OracleParameterCollection param);
        OracleDB Db { get; }
        string TableName { get; }
    }

    public interface IQueryContext<T> : IQueryContext, IEnumerable<T>
    {
    }
}
