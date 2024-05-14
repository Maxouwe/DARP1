using System.Collections;
using System.Data.SQLite;
using System.IO;

namespace Programma1
{
    internal class Program
    {
        static string metaConnectionString = @"Data Source=..\..\..\db\meta.db;Version=3";
        static string intermediatesConnectionString = @"Data Source=..\..\..\db\intermediates.db;Version=3";
        static int numTuples;

        static void Main(string[] args)
        {
            //delete metadatabase als die er nog is van een vorige keer
            if (File.Exists(@"..\..\..\db\meta.db"))
            {
                File.Delete(@"..\..\..\db\meta.db");
            }

            //create the database files
            SQLiteConnection.CreateFile(@"..\..\..\db\meta.db");

            //make the connections
            SQLiteConnection metaConnection = new SQLiteConnection(metaConnectionString);
            metaConnection.Open();


            //create intermediateresults table
            createIntermediateTables(metaConnection);
            
            //retrieve the amount of tuples in autompg
            using (SQLiteCommand command = new SQLiteCommand(metaConnection))
            {
                command.CommandText = @"SELECT COUNT(*) AS c FROM autompg";
                using (SQLiteDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        numTuples = reader.GetInt32(reader.GetOrdinal("c"));
                    }
                }
            }

            //put categorical IDF in the intermediate tables
            collectCategoricalIDF(metaConnection);
            
            //dictionary with the QFs of all attribute values
            //numerical QF are still handled as categorical QFs
            collectQF(metaConnection);

            //retrieve the qf and idf from their tables and fill the qfidf tables inside the metadb
            using (SQLiteCommand command = new SQLiteCommand(metaConnection))
            {
                command.CommandText = File.ReadAllText(@"..\..\..\db\metaload.txt");
                command.ExecuteNonQuery();
            }
            
           

            using (SQLiteCommand command = new SQLiteCommand(metaConnection))
            {
                //now we do the same but with a select statement
                command.CommandText = @"SELECT * FROM brandqfidf";

                //make a reader that will read the db and retrieve the tuples we asked for
                //de using statment zorgt dat zodra we uit de scope van reader zijn, de reader niet meer actief is
                using (SQLiteDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine(reader.GetString(reader.GetOrdinal("brand")) + reader.GetFloat(reader.GetOrdinal("qfidf")));
                    }
                }
            }
        }

        //verwijdert alle tables die gebruikt zijn voor intermediate results
        static void deleteIntermediateTables(SQLiteConnection connection)
        {

        }
        //finds all QFs and puts them in the table specified through the connection parameter
        static void collectQF(SQLiteConnection connection)
        {
            string[] lines = File.ReadLines(@"..\..\..\db\workload.txt").ToArray();

            int RQFMax = 0;

            //make dictionary so we can look up currently known RQF of an attribute by the attribute name
            Dictionary<string, float> RQFs = new Dictionary<string, float>();

            //parse all lines
            //and put it in the dictionary with format ["attribute=value", rqf(value)]
            for (int i = 2; i < lines.Length; i++)
            {
                if (lines[i] != "")
                {
                    
                    string[] line = lines[i].Trim().Split('=');

                    int freq = Int32.Parse(line[0].Split(' ')[0]);

                    
                    for(int j = 0; j < line.Length - 1;j++)
                    {
                        string[] subline = line[j].Trim().Split(' ');

                        string key = subline[subline.Length - 1] + "=" + line[j + 1].Split('\'')[1];
                        

                        if (RQFs.ContainsKey(key))
                        {
                            RQFs[key] = RQFs[key] + freq;
                        }
                        else
                        {
                            RQFs[key] = freq;
                        }

                        if (RQFs[key] > RQFMax)
                        {
                            RQFMax = (int)RQFs[key];
                        }
                    }
                }
            }

            //now we know all RQFs and RQFMax so we can compute the QFs
            //we will replace the RQFs by their QFs
            //for now the numerical QF is handled like categorical QF, change later
            foreach(KeyValuePair<string, float> pair in RQFs)
            {
                RQFs[pair.Key] = pair.Value / RQFMax; 
            }
            //remember despite the dictionary being called RQFs it now contains all the QFs instead

            //insert them into the QF tables
            using (SQLiteCommand command = new SQLiteCommand(connection))
            {
                foreach (KeyValuePair<string, float> pair in RQFs)
                {
                    
                    string attribute = pair.Key.Split('=')[0];
                    string attributeValue = pair.Key.Split('=')[1];
                    command.CommandText = String.Format(@"INSERT INTO {0}qf VALUES({1}, {2})", attribute, "\'" + attributeValue + "\'", pair.Value);
                    command.ExecuteNonQuery();
                }
            }


        }
 
        static void collectCategoricalIDF(SQLiteConnection connection)
        {

            //collect idf for the brand attribute
            string sqlStatements = String.Format(@"INSERT INTO brandidf SELECT brand, LOG({0}/COUNT(brand)) FROM autompg GROUP BY brand", numTuples);
            using (SQLiteCommand command = new SQLiteCommand(connection))
            {
                command.CommandText = sqlStatements;
                command.ExecuteNonQuery();
            }

            //collect idf for the model attribute
            sqlStatements = String.Format(@"INSERT INTO modelidf SELECT model, LOG({0}/COUNT(model)) FROM autompg GROUP BY model", numTuples);
            using (SQLiteCommand command = new SQLiteCommand(connection))
            {
                command.CommandText = sqlStatements;
                command.ExecuteNonQuery();
            }

            //collect idf for the type attribute
            sqlStatements = String.Format(@"INSERT INTO typeidf SELECT type, LOG({0}/COUNT(type)) FROM autompg GROUP BY type", numTuples);
            using (SQLiteCommand command = new SQLiteCommand(connection))
            {
                command.CommandText = sqlStatements;
                command.ExecuteNonQuery();
            }
        }

        static void createIntermediateTables(SQLiteConnection connection)
        {
            using (SQLiteCommand command = new SQLiteCommand(connection))
            {
                //create the autompg table
                string sqlStatements = File.ReadAllText(@"..\..\..\db\autompg.sql");
                command.CommandText = sqlStatements;
                command.ExecuteNonQuery();

                //create the intermediate results tables
                command.CommandText = File.ReadAllText(@"..\..\..\db\intermediates.txt");
                command.ExecuteNonQuery();
            }
        }
    }
}