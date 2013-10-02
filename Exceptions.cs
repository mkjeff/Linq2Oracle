using System;

namespace Linq2Oracle
{
    [Serializable]
    public sealed class DalException : ApplicationException
    {
        public readonly DbErrorCode ErrorCode;
        public DalException(DbErrorCode errorCode, string msg)
            : base(msg)
        {
            ErrorCode = errorCode;
        }
    }

    public enum DbErrorCode
    {
        E_DB_EXEC_PROCEDURE_FAIL,
        E_DB_PK_NULL,
        E_DB_SQL_INVAILD,
        E_DB_INSERT_FAIL,
        E_DB_UNKNOWN_DATATYPE,
        E_DB_NOT_SUPPORT_OPERATOR,
        E_DB_OVER_COLUMN_SIZE
    }
}
