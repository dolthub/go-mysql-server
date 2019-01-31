using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySql.Data.MySqlClient;
using System.Threading.Tasks;

namespace dotnet
{
    [TestClass]
    public class MySQLTest
    {
        [TestMethod]
        public async Task TestCanConnect()
        {
            var connectionString = "server=127.0.0.1;user id=root;password=;port=3306;database=mydb;";
            var expected = new string[][]{
                new string[]{"Evil Bob", "evilbob@gmail.com"},
                new string[]{"Jane Doe", "jane@doe.com"},
                new string[]{"John Doe", "john@doe.com"},
                new string[]{"John Doe", "johnalt@doe.com"},
            };

            using (var conn = new MySqlConnection(connectionString))
            {
                await conn.OpenAsync();

                var sql = "SELECT name, email FROM mytable ORDER BY name, email";
                var i = 0;

                using (var cmd = new MySqlCommand(sql, conn))
                using (var reader = await cmd.ExecuteReaderAsync())
                while (await reader.ReadAsync()) {
                    if (i >= expected.Length) {
                        Assert.Fail("more rows than expected");
                    }

                    Assert.AreEqual(expected[i][0], reader.GetString(0));
                    Assert.AreEqual(expected[i][1], reader.GetString(1));
                    i++;
                }
            }
        }
    }
}
