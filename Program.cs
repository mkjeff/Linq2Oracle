using Linq2Oracle;
using Linq2Oracle.Expressions;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using Test.DataModel;
using Test.DataModel.Code;
using Test.Properties;

namespace NewNew
{
    static class Program
    {
        static void Main(string[] args)
        {
            const string connectionString = "User Id=username;Password=passwd;data source=//server:port/SID";
            using (var conn = new OracleConnection(connectionString))
            {
                var db = new DbContext(conn, Console.Out);

            }
        }

    }

}
