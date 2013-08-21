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
                int? q1 = db.N_USER.Max(a => a.LOGIN_COUNT);


                int? i = null;
                var q2 = db.N_USER
                            .Where(a => a.DEPT_CODE + "A" == null)
                            .Where(a => a.LOGIN_COUNT.GetValueOrDefault(0) == i);
                            //.GroupBy(a => a.DEPT_CODE)
                            //.Where(g => g.Count() == 1) //&& g.Sum(a=>a.LOGIN_COUNT) >100)
                            //.Select(g => new
                            //{
                            //    Department = g.Key,
                            //    Count = g.Count(),
                            //});
            }
        }

    }

}
