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
        static T Then<T>(this bool condition, Func<T> valueProvider) where T:struct
        {
            return condition ? valueProvider() : default(T);
        }

        static bool HasValue(this string str)
        {
            return !string.IsNullOrEmpty(str);
        }

        static void Main(string[] args)
        {
            const string connectionString = "User Id=username;Password=passwd;data source=//server:port/SID";
            using (var conn = new OracleConnection(connectionString))
            {
                var dept = "";
                var db = new DbContext(conn,Console.Out);
                // flowing statement will compiler error:
                var q = from a in db.N_USER
                        where dept.HasValue().Then(() => a.DEPT_CODE == dept) // optional predicate when dept.HasValue()
                        group a by a.DEPT_CODE into g
                        where g.Any(a => a.AD_FLAG == Flag.N)
                        select new
                        {
                            Department = g.Key,
                            Count = g.Count(),
                        };

                var q2 = from a in db.N_USER
                         group a by a.DEPT_CODE;

                foreach (var g in q2)
                {
                    var q21 = g.Where(a => a.DEPT_CODE == "");
                }
            }
        }

    }

}
