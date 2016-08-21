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
                var db = new DbContextCode(conn, Console.Out);
                var query =
                    from a in db.N_USER
                    where a.AD_FLAG == Flag.Y
                    where a.AGE >= 18
                    group a by a.DEPT_CODE into g
                    where g.Average(a=>a.AGE) > 30
                    select new
                    {
                        g.Key,
                        g,
                    };

                var count = query.Count();
                if (count > 10)
                {

                }
            }
        }

    }

}
