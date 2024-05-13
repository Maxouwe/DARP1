using System.Data.SQLite;
using System.IO;

namespace Programma1
{
    internal class Program
    {
        static void Main(string[] args)
        {
            //delete database als die er nog is van een vorige keer
            if (File.Exists(@"..\..\..\meta.db"))
            {
                File.Delete(@"..\..\..\meta.db");
            }
        }

        //find and store the idf for all categorical attributes into a table
        static void processCatIDF()
        {

        }

        //find and store the idf for all numerical attributes into a table
        static void processNumIDF()
        {

        }

        //process the workload to find the QF of all categorical attributes
        //probably need to merge later with processNumQF
        //so we dont need to scan the workload twice
        static void processCatQF()
        {

        }

        //process the workload to find the QF of all numerical attributes
        static void processNumQF()
        {

        }
    }
}