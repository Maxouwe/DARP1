using System.Data.SQLite;
using System.IO;
using System.Reflection.PortableExecutable;

namespace testDB
{
    internal class Program
    {
        static string connectionString = @"Data Source=..\..\..\test.db;Version=3";

        static void Main(string[] args)
        {
            //delete database als die er nog is van een vorige keer
            if(File.Exists(@"..\..\..\db\test.db"))
            {
                File.Delete(@"..\..\..\db\test.db");
            }

            //create the database file
            SQLiteConnection.CreateFile(@"..\..\..\test.db");

            //make a connection
            SQLiteConnection connection = new SQLiteConnection(connectionString);
            connection.Open();


            //we can read from a text file filled with sql statements
            //this file contains create table and insert statements
            string sqlStatements = File.ReadAllText(@"..\..\..\autompg.sql");

            //make a sql command
            SQLiteCommand command = new SQLiteCommand(connection);
            command.CommandText = sqlStatements;
            //and execute the sql statements from the text file
            command.ExecuteNonQuery();


            //now we do the same but with a select statement
            command.CommandText = File.ReadAllText(@"..\..\..\testquery.sql");

            //make a reader that will read the db and retrieve the tuples we asked for
            //de using statment zorgt dat zodra we uit de scope van reader zijn, de reader niet meer actief is
            using (SQLiteDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    Console.WriteLine(reader.GetInt32(reader.GetOrdinal("model_year")));
                }
            }

            //we can also perform a sql statement like this
            //the @ is important especially when there are multiple lines
            command.CommandText = @"SELECT * FROM autompg WHERE model_year = '82' AND type = 'sedan'";

            using (SQLiteDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    //print all the brands of the found tuples
                    Console.WriteLine(reader.GetString(reader.GetOrdinal("brand")));
                }
            }

        }
    }
}