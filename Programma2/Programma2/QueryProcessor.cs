using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data.SQLite;
using System.Data.SqlClient;

namespace Programma2
{
    //this class will use a query object to process the query
    //by making sure all the needed tables are created
    //joining all the tables where necessary
    //make a topk selection if many answers
    //weaken the constraints if zero answers/not enough answers
    class QueryProcessor
    {
        private Query _query;
        private int _k;
        public QueryProcessor(Query query, int k)
        {
            _query = query;
            _k = k; 
        }

        //create a table where all qfidf score tables are joined together with the autompg table
        //they are joined on the autompg.attribute = qfidftable.attribute
        private void createQFIDFAnswerTuplesTable(SQLiteConnection connection)
        {
            //invoke _query.createTables()
            //join them and select all tuples where all attributes=queryterms
        }

        //the code for joining of tables done in createQFIDFAnswerTuplesTable()
        //is put together in this function
        private void joinTables(SQLiteConnection connection)
        {

        }
        //remove the constraint that type = queryValue
        private void leaveOutType(SQLiteConnection connection)
        {
            
        }
        //instead of searching for tuples where the numerical attributes are exactly equal to the queryvalues
        //we add margins: select where A-h =< A =< A + h  (A is the attribute, h is the h parameter of that attribute)
        private void addMargins(SQLiteConnection connection)
        {

        }

        //increases the margins from h to 2*h
        private void increaseMargins(SQLiteConnection connection)
        {

        }

        private void searchJustByBrand(SQLiteConnection connection)
        {

        }

        private void searchJustByModel(SQLiteConnection connection)
        {

        }

        private void searchJustByWeakenedNumericalConstraints(SQLiteConnection connection)
        {

        }

        private void searchByEnvironmentFunction(SQLiteConnection connection)
        {

        }

        public void findTopK(SQLiteConnection connection)
        {
            //createQFIDFAnswerTuplesTable()
            //order table by QFIDF score
            //if amount of retrieved tuples < k
            //    -first try to leave out type if it is specified
            //    -then change numerical where clause to WHERE A-h<=A<=A+h
            //    -then change numerical where clause to WHERE A-2h<=A<=A+2h
            //    -then leave out all numerical constraints
            //    -then search just by brand
            //    -else search just by model
            //    -else search from all cars with the weakened numerical constraints
            //    -else select from all cars based on an environmentally friendly function
            //else
            //    -return so we can print the tuples from the retrieved table from the main function
        }
    }
}
