using System.Collections;
using System.Data.SQLite;
using System.IO;

namespace Programma1
{
    internal class Program
    {
        static string metaConnectionString = @"Data Source=..\..\..\db\meta.db;Version=3";
        static string intermediatesConnectionString = @"Data Source=..\..\..\db\intermediates.db;Version=3";
        static string[] attributes = { "mpg", "cylinders", "displacement", "horsepower", "weight", "acceleration", "model_year", "origin", "brand", "model", "type"};

        static void Main(string[] args)
        {
            //delete metadatabase als die er nog is van een vorige keer
            if (File.Exists(@"..\..\..\db\meta.db"))
            {
                File.Delete(@"..\..\..\db\meta.db");
            }

            //delete intermediate results database als die er nog is
            if(File.Exists(@"..\..\..\db\intermediates.db"))
            {
                File.Delete(@"..\..\..\db\intermediates.db");
            }

            //create the database files
            SQLiteConnection.CreateFile(@"..\..\..\db\meta.db");
            SQLiteConnection.CreateFile(@"..\..\..\db\intermediates.db");

            //make the connections
            SQLiteConnection metaConnection = new SQLiteConnection(metaConnectionString);
            SQLiteConnection intermediatesConnection = new SQLiteConnection(intermediatesConnectionString);
            metaConnection.Open();
            intermediatesConnection.Open();

            //create intermediateresults table
            createIntermediateTables(intermediatesConnection);
            //put categorical IDF in the intermediate tables
            collectCategoricalIDF(intermediatesConnection);
            
            //dictionary with the QFs of all attribute values
            //numerical QF are still handled as categorical QFs
            Dictionary<string, float> dict = collectQF(intermediatesConnection);

            foreach (KeyValuePair<string, float> pair in dict)
            {
                Console.WriteLine("QF({0}) = {1}", pair.Key, pair.Value);
            }

        }

        //returns the QFs of all attribute values
        static Dictionary<string, float> collectQF(SQLiteConnection connection)
        {
            string[] lines = File.ReadLines(@"..\..\..\db\workload.txt").ToArray();

            int RQFMax = 0;

            //make dictionary so we can look up currently known RQF of an attribute by the attribute name
            Dictionary<string, float> RQFs = new Dictionary<string, float>();

            //parse all lines
            for (int i = 2; i < lines.Length; i++)
            {
                if (lines[i] != "")
                {
                    //the word/symbol number of the line
                    int wordIndex = 0;

                    string[] line = lines[i].Trim().Split(' ');

                    int freq = Int32.Parse(line[0]);

                    while (wordIndex < line.Length)
                    {
                        //als het tegengekomen woord een attribuut is 
                        if (attributes.Contains(line[wordIndex]))
                        {
                            //dont parse the IN clauses
                            if (line[wordIndex + 1] != "IN")
                            {

                                string key = line[wordIndex] + " = " + line[wordIndex + 2];

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

                                wordIndex = wordIndex + 2;
                            }
                        }
                        wordIndex++;
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
            return RQFs;
        }

        static void collectCategoricalIDF(SQLiteConnection connection)
        {
            //collect idf for the brand attribute
            string sqlStatements = @"INSERT INTO brandidf SELECT brand, COUNT(brand) FROM autompg GROUP BY brand";
            using (SQLiteCommand command = new SQLiteCommand(connection))
            {
                command.CommandText = sqlStatements;
                command.ExecuteNonQuery();
            }

            //collect idf for the model attribute
            sqlStatements = @"INSERT INTO modelidf SELECT model, COUNT(model) FROM autompg GROUP BY model";
            using (SQLiteCommand command = new SQLiteCommand(connection))
            {
                command.CommandText = sqlStatements;
                command.ExecuteNonQuery();
            }

            //collect idf for the type attribute
            sqlStatements = @"INSERT INTO typeidf SELECT type, COUNT(type) FROM autompg GROUP BY type";
            using (SQLiteCommand command = new SQLiteCommand(connection))
            {
                command.CommandText = sqlStatements;
                command.ExecuteNonQuery();
            }
        }

        static void createIntermediateTables(SQLiteConnection connection)
        {
            //create the autompg table
            string sqlStatements = File.ReadAllText(@"..\..\..\db\autompg.sql");
            using (SQLiteCommand command = new SQLiteCommand(connection))
            {
                command.CommandText = sqlStatements;
                command.ExecuteNonQuery();
            }

            //create brandidf table
            sqlStatements = @"CREATE TABLE brandidf(brand text, idf integer, PRIMARY KEY(brand));";
            using (SQLiteCommand command = new SQLiteCommand(connection))
            {
                command.CommandText = sqlStatements;
                command.ExecuteNonQuery();
            }

            //create modelidf table
            sqlStatements = @"CREATE TABLE modelidf(model text, idf integer, PRIMARY KEY(model));";
            using (SQLiteCommand command = new SQLiteCommand(connection))
            {
                command.CommandText = sqlStatements;
                command.ExecuteNonQuery();
            }

            //create typeidf table
            sqlStatements = @"CREATE TABLE typeidf(type text, idf integer, PRIMARY KEY(type));";
            using (SQLiteCommand command = new SQLiteCommand(connection))
            {
                command.CommandText = sqlStatements;
                command.ExecuteNonQuery();
            }
        }
    }
}