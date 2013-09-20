using Oracle.ManagedDataAccess.Client;
using System;

namespace Linq2Oracle
{
    [AttributeUsage(AttributeTargets.Property, Inherited = false)]
    public sealed class ColumnAttribute : Attribute
    {
        public bool IsPrimarykey { get; set; }
        public bool IsNullable { get; set; }
        public OracleDbType DbType { get; set; }
        public int Size { get; set; }
    }

    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
    public sealed class ConcurrencyCheckAttribute : Attribute
    {
        public ConcurrencyCheckAttribute(string columnName)
        {
            this.ColumnName = columnName;
        }
        public string ColumnName { get; private set; }
    }
}
