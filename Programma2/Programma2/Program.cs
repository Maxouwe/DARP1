using System.Data.SQLite;

namespace Programma2
{
    internal class Program
    {
        static string metaConnectionString = @"Data Source=..\..\..\..\..\db\meta.db;Version=3";
        static int numTuples;
        static string[] attributes = {"mpg", "cylinders", "displacement", "horsepower", "weight", "acceleration", "model_year", "origin", "brand", "model", "type"};

        static void Main(string[] args)
        {

            //make a connection with the database
            SQLiteConnection metaConnection = new SQLiteConnection(metaConnectionString);
            metaConnection.Open();


            //get the amount of tuples(needed for multiple calculations)
            SQLiteUtilities.readTuples(metaConnection,
                @"SELECT * FROM numtuples",
                delegate (SQLiteDataReader reader)
                {
                    numTuples = reader.GetInt32(reader.GetOrdinal("num"));
                }
                );

            while (true)
            {
                Console.WriteLine("Enter query according to the format");
                
                QueryProcessor processor = parseInput(Console.ReadLine(), numTuples, metaConnection);

                //input contained attributes that are not in autompg
                if(processor == null)
                {
                    Console.WriteLine("Only enter attributes that are in the table");
                }

                else
                {
                    processor.deleteTables(metaConnection);
                    processor.findTopK(metaConnection);

                    SQLiteUtilities.readTuples(metaConnection,
                    @"SELECT * FROM topk",
                    delegate (SQLiteDataReader reader)
                    {
                        object[] values = new object[processor._query.numTerms.Count + processor._query.catTerms.Count + attributes.Length + 1];
                        reader.GetValues(values);
                        for(int i = 0; i < values.Length; i++)
                        {
                            Console.Write(values[i] + " | ");
                        }
                        Console.WriteLine();
                    });

                    processor.deleteTables(metaConnection);
                }

                

            }

            //k = 6, brand = 'ford', cylinders = 7;
        }

        static QueryProcessor? parseInput(string input, int numTuples, SQLiteConnection connection)
        {
            int k = 10;

            string[] subqueries = input.Trim().Split(',');

            List<NumericalAttribute> numTerms = new List<NumericalAttribute>();
            List<CategoricalAttribute> catTerms = new List<CategoricalAttribute>();

            for (int i = 0; i < subqueries.Length; i++)
            {
                string attribute = subqueries[i].Split('=')[0].Trim();
                string value = subqueries[i].Trim().Split('=')[1].Trim();

                if (value[value.Length - 1] == ';')
                {
                    value = value.Substring(0, value.Length - 1);
                }

                int l = 0;
                if (attribute == "k")
                {
                    k = Int32.Parse(value);
                }

                else if (!attributes.Contains(attribute))
                {
                    return null;
                }

                //if the value is not in quotes i.e. it is a numerical attribute
                else if (value[0] != '\'')
                {
                    numTerms.Add(new NumericalAttribute(attribute, float.Parse(value), numTuples, connection));
                }

                //if categorical attribute
                else
                {
                    catTerms.Add(new CategoricalAttribute(attribute, value));
                }
            }

            return new QueryProcessor(new Query(numTerms, catTerms), k);
        }

        static void processQuery()
        {

        }
    }
}