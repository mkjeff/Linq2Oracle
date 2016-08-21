using System;
using System.Linq;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Test.DataModel.Code;

namespace NewNew
{
    static class Program
    {
        static void Main()
        {



        }

        static async Task Test()
        {
            const string connectionString = "User Id=username;Password=passwd;data source=//server:port/SID";
            using (var conn = new OracleConnection(connectionString))
            {
                var db = new DbContextCode(conn, Console.Out);
                var query =
                    from a in db.N_USER
                    where a.AD_FLAG == Flag.Y
                    where a.LOGIN_COUNT > 1000
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

                var sum = await db.N_USER.SumAsync(a => a.AGE);
                var min = db.N_USER.Max(a => a.AGE);
                if (min > 10)
                {

                }
                min = new N_USER().AGE;
            }
        }

    }

}
