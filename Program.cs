using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using Test.Properties;
using TestDatabase;

namespace NewNew
{
    static class Program
    {
        static void Main(string[] args)
        {
            const string connectionString = "User Id=username;Password=passwd;data source=//server:port/SID";
            using (var conn = new OracleConnection(connectionString))
            {
                var db = new DbContext(conn,Console.Out);

                var query = from a in db.N_USER
                            where a.LOCK_FLAG == Flag.N
                            group a by a.DEPT_CODE into g
                            where g.Count() < 50
                            select g;

                foreach (var g in query)
                {
                    Console.WriteLine(g.Key);
                    foreach (var u in g)
                        Console.WriteLine(u);
                }

                // flowing statement will compiler error:
                //var q = from a in db.N_USER
                //        group new { a.DEPT_CODE, a.LANG} by a.DEPT_CODE into g
                //        select new
                //        {
                //            Department = g.Key,
                //            Count = g.Count(),
                //        };
            }
        }

    }

}
