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
            using (var conn = new OracleConnection(Settings.Default.DB))
            {
                var db = new DbContext(conn,Console.Out);

                var query = from a in db.N_USER
                            let b = a.AD_FLAG
                            where a.LANG == null
                            where a.LOCK_FLAG == Flag.N
                            select new
                            {
                                a.USER_ID,
                                a.USER_NAME,
                                a.UPDATE_DATE,
                                b,
                            };

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
