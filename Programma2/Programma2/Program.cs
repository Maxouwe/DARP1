using System.Data.SQLite;

namespace Programma2
{
    internal class Program
    {
        static string metaConnectionString = @"Data Source=..\..\..\..\..\db\meta.db;Version=3";
        static int numTuples;

        static void Main(string[] args)
        {
            //make a connection with the database
            SQLiteConnection metaConnection = new SQLiteConnection(metaConnectionString);
            metaConnection.Open();

            SQLiteUtilities.readTuples(metaConnection,
                @"SELECT COUNT(*) AS amountOfTuples FROM autompg",
                delegate (SQLiteDataReader reader)
                {
                    numTuples = reader.GetInt32(reader.GetOrdinal("amountOfTuples"));
                }
                );
            NumericalAttribute att = new NumericalAttribute("mpg", 15, numTuples, metaConnection);

            att.deleteTables(metaConnection);
            att.createQFIDFTable(metaConnection);

            SQLiteUtilities.readTuples(metaConnection,
                @"SELECT * FROM mpgqfidf",
                delegate (SQLiteDataReader reader)
                {
                    Console.WriteLine(reader.GetFloat(reader.GetOrdinal("mpg")) + "=" + reader.GetFloat(reader.GetOrdinal("qfidf")));
                });
        }
    }
}