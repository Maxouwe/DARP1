using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data.SQLite;
using System.Data.SqlClient;

//so far we have joined autompg with all the qfidf tables
//now we need to deal with zero and many answers and find the top-k
//first test the createQFIDFAnswerTuplesTable and then
//continue with the calculate top-k function
namespace Programma2
{
    //this class will use a query object to process the query
    //by making sure all the needed tables are created
    //joining all the tables where necessary
    //make a topk selection if many answers
    //weaken the constraints if zero answers/not enough answers
    class QueryProcessor
    {
        public Query _query;
        private int _k;

        public QueryProcessor(Query query, int k)
        {
            _query = query;
            _k = k; 
        }

        public void deleteTables(SQLiteConnection connection)
        {
            _query.removeTables(connection);
            SQLiteUtilities.executeSQL(connection, @"DROP TABLE IF EXISTS allqfidf; DROP TABLE IF EXISTS topk;");
        }
        //create a table where all qfidf score tables are joined together with the autompg table
        //they are joined on the autompg.attribute = qfidftable.attribute
        
        private void createQFIDFAnswerTuplesTable(SQLiteConnection connection)
        {
            _query.createTables(connection);

            //join them and select all tuples where all attributes=queryterms
            //they are put into a table called resulttable
            joinTables(connection);
        }

        //the code for joining of tables done in createQFIDFAnswerTuplesTable()
        //is put together in this function
        private void joinTables(SQLiteConnection connection)
        {
            //create the string that will hold all the attributes columns + the columns for all the qfidf of the attributes mentioned in the query
            //using select * produces duplicate columns, so we have to mention each individual column
            string columnListString = "autompg.id, autompg.mpg, autompg.cylinders, autompg.displacement, autompg.horsepower, autompg.weight, autompg.acceleration, autompg.model_year, autompg.origin, autompg.brand, autompg.model, autompg.type";
            //this string will be a concatenation of "INNER JOIN on attributeqfidf.attribute = autompg.attribute" for each attribute
            string joinString = "";

            foreach(NumericalAttribute att in _query.numTerms)
            {
                columnListString = addColumnNameNum(columnListString, att);
                joinString = addJoinStringNum(joinString, att); 
            }

            foreach (CategoricalAttribute att in _query.catTerms)
            {
                columnListString = addColumnNameCat(columnListString, att);
                joinString = addJoinStringCat(joinString, att);
            }


            SQLiteUtilities.executeSQL(connection, String.Format(@"CREATE TABLE allqfidf AS SELECT {0} FROM autompg {1}", columnListString, joinString));
        }

        //used to expand the columnliststring based on an given attribute
        //for numerical atributes
        private string addColumnNameNum(string columnListString, NumericalAttribute att)
        {
            return String.Format(columnListString + ", {0}qfidf.qfidf AS {0}qfidf", att.attributeName);
        }
        //used to expand the columnliststring based on an given attribute
        //for categorical atributes
        private string addColumnNameCat(string columnListString, CategoricalAttribute att)
        {
            return String.Format(columnListString + ", {0}qfidf.qfidf AS {0}qfidf", att.attributeName);
        }
        //used to expand the inner join string based on a given attribute
        //for numerical attributes
        private string addJoinStringNum(string joinString, NumericalAttribute att)
        {
            return String.Format(joinString + " INNER JOIN {0}qfidf ON {0}qfidf.{0} = autompg.{0}", att.attributeName);
        }
        //used to expand the inner join string based on a given attribute
        //for categorical attributes

        private string addJoinStringCat(string joinString, CategoricalAttribute att)
        {
            return String.Format(joinString + " INNER JOIN {0}qfidf ON {0}qfidf.{0} = autompg.{0}", att.attributeName);
        }

        //get the amount of tuples in topk
        private int getCountTopk(SQLiteConnection connection)
        {
            int count = 0;
            SQLiteUtilities.readTuples(connection, @"SELECT COUNT(*) AS c FROM topk",
                delegate (SQLiteDataReader reader)
                {
                    count = reader.GetInt32(reader.GetOrdinal("c"));
                }
                );
            return count;
        }


        //adds additional ranking by applying a function
        //incase we get alot of tuples with the same qfidf
        //the additional ranking function is horsepower*3 + acceleration*40 + mpg*30 + model_year * 30
        //this function is explained in the supplied explanation text file
        private void additionalRanking(SQLiteConnection connection)
        {
            replaceTopkTable(connection, String.Format(@"SELECT *, (horsepower*3 + acceleration*40 + mpg*30 + model_year * 30) AS func FROM topk ORDER BY horsepower*3 + acceleration*40 + mpg*30 + model_year * 30 DESC LIMIT {0}", _k));
        }
        //remove the constraint that type = queryValue
        private void leaveOutType(SQLiteConnection connection)
        {
            
        }
        //instead of searching for tuples where the numerical attributes are exactly equal to the queryvalues
        //we add margins: select where A-m*h =< q =< A + m*h  (A is the attribute, h is the h parameter of that attribute)
        private void addMargins(SQLiteConnection connection, int m)
        {
            string columnNames = "allqfidf.id, allqfidf.mpg, allqfidf.cylinders, allqfidf.displacement, allqfidf.horsepower, allqfidf.weight, allqfidf.acceleration, allqfidf.model_year, allqfidf.origin, allqfidf.brand, allqfidf.model, allqfidf.type";
            string ceq = "";
            string sumQFIDFColumn = ", (";

            for (int i = 0; i < _query.numTerms.Count; i++)
            {
                if (i == _query.numTerms.Count - 1 && _query.catTerms.Count == 0)
                {
                    ceq += String.Format("{2} BETWEEN {0}-{3}*{1} and {0}+{3}*{1}", _query.numTerms[i].attributeName, _query.numTerms[i].h, _query.numTerms[i].queryValue, m);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf", _query.numTerms[i].attributeName);
                }
                else
                {
                    ceq += String.Format("{2} BETWEEN {0}-{3}*{1} and {0}+{3}*{1} AND ", _query.numTerms[i].attributeName, _query.numTerms[i].h, _query.numTerms[i].queryValue, m);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf + ", _query.numTerms[i].attributeName);
                }
            }

            //still keep the equality for the categorical terms
            for (int i = 0; i < _query.catTerms.Count; i++)
            {
                if (i == _query.catTerms.Count - 1)
                {
                    ceq += String.Format("{0} = {1}", _query.catTerms[i].attributeName, _query.catTerms[i].queryValue);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf", _query.numTerms[i].attributeName);
                }
                else
                {
                    ceq += String.Format("{0} = {1} AND ", _query.catTerms[i].attributeName, _query.catTerms[i].queryValue);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf + ", _query.numTerms[i].attributeName);
                }
            }

            sumQFIDFColumn += ") AS sumqfidf";
            columnNames += sumQFIDFColumn;

            replaceTopkTable(connection, String.Format(@"SELECT {0} FROM allqfidf WHERE {1} ORDER BY sumqfidf DESC", columnNames, ceq));
        }

        //search by a single categorical attribute and numerical contraints with margins m*h
        private void searchJustByAttributeAndMargins(SQLiteConnection connection, int m, CategoricalAttribute? cat)
        {
            string columnNames = "allqfidf.id, allqfidf.mpg, allqfidf.cylinders, allqfidf.displacement, allqfidf.horsepower, allqfidf.weight, allqfidf.acceleration, allqfidf.model_year, allqfidf.origin, allqfidf.brand, allqfidf.model, allqfidf.type";
            string ceq = "";
            string sumQFIDFColumn = ", (";

            for (int i = 0; i < _query.numTerms.Count; i++)
            {
                ceq += String.Format("{2} BETWEEN {0}-{3}*{1} and {0}+{3}*{1} AND ", _query.numTerms[i].attributeName, _query.numTerms[i].h, _query.numTerms[i].queryValue, m);
                sumQFIDFColumn += String.Format("allqfidf.{0}qfidf + ", _query.numTerms[i].attributeName);
            }

            ceq += String.Format("{0} = {1}", cat.attributeName, cat.queryValue);
            sumQFIDFColumn += String.Format("allqfidf.{0}qfidf", cat.attributeName);
            sumQFIDFColumn += ") AS sumqfidf";
            columnNames += sumQFIDFColumn;

            replaceTopkTable(connection, String.Format(@"SELECT {0} FROM allqfidf WHERE {1} ORDER BY sumqfidf DESC", columnNames, ceq));
        }

        private void searchJustByMargins(SQLiteConnection connection, int m)
        {
            string columnNames = "allqfidf.id, allqfidf.mpg, allqfidf.cylinders, allqfidf.displacement, allqfidf.horsepower, allqfidf.weight, allqfidf.acceleration, allqfidf.model_year, allqfidf.origin, allqfidf.brand, allqfidf.model, allqfidf.type";
            string ceq = "";
            string sumQFIDFColumn = ", (";

            for (int i = 0; i < _query.numTerms.Count; i++)
            {
                if (i == _query.numTerms.Count - 1 && _query.catTerms.Count == 0)
                {
                    ceq += String.Format("{2} BETWEEN {0}-{3}*{1} and {0}+{3}*{1}", _query.numTerms[i].attributeName, _query.numTerms[i].h, _query.numTerms[i].queryValue, m);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf", _query.numTerms[i].attributeName);
                }
                else
                {
                    ceq += String.Format("{2} BETWEEN {0}-{3}*{1} and {0}+{3}*{1} AND ", _query.numTerms[i].attributeName, _query.numTerms[i].h, _query.numTerms[i].queryValue, m);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf + ", _query.numTerms[i].attributeName);
                }
            }

            sumQFIDFColumn += ") AS sumqfidf";
            columnNames += sumQFIDFColumn;

            replaceTopkTable(connection, String.Format(@"SELECT {0} FROM allqfidf WHERE {1} ORDER BY sumqfidf DESC", columnNames, ceq));
        }


        //completely repopulates the topk table
        //based on the given select statement
        private void replaceTopkTable(SQLiteConnection connection, string selectStatement)
        {
            //create temp table with topk, drop the topk table, create the topk table filled with the new topk, then delete the temporary table
            SQLiteUtilities.executeSQL(connection, String.Format(@"CREATE TABLE temp AS {0}; DROP TABLE topk; CREATE TABLE topk AS SELECT * FROM temp; DROP TABLE temp", selectStatement));
        }
         
        public void findTopK(SQLiteConnection connection)
        {
            //create the autompg table joined with all the qfidf tables
            createQFIDFAnswerTuplesTable(connection);

            string columnNames = "allqfidf.id, allqfidf.mpg, allqfidf.cylinders, allqfidf.displacement, allqfidf.horsepower, allqfidf.weight, allqfidf.acceleration, allqfidf.model_year, allqfidf.origin, allqfidf.brand, allqfidf.model, allqfidf.type";
            string ceq = "";
            string sumQFIDFColumn = ", (";

            //prepare all strings needed in the select query
            for (int i = 0; i < _query.numTerms.Count; i++)
            {
                if(i == _query.numTerms.Count - 1 && _query.catTerms.Count == 0)
                {
                    ceq += String.Format("{0} = {1}", _query.numTerms[i].attributeName, _query.numTerms[i].queryValue);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf", _query.numTerms[i].attributeName);
                }
                else
                {
                    ceq += String.Format("{0} = {1} AND ", _query.numTerms[i].attributeName, _query.numTerms[i].queryValue);
                    sumQFIDFColumn  += String.Format("allqfidf.{0}qfidf + ", _query.numTerms[i].attributeName);
                }
            }

            for (int i = 0; i < _query.catTerms.Count; i++)
            {
                if (i == _query.catTerms.Count - 1)
                {
                    ceq += String.Format("{0} = {1}", _query.catTerms[i].attributeName, _query.catTerms[i].queryValue);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf", _query.numTerms[i].attributeName);
                }
                else
                {
                    ceq += String.Format("{0} = {1} AND ", _query.catTerms[i].attributeName, _query.catTerms[i].queryValue);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf + ", _query.numTerms[i].attributeName);
                }
            }

            sumQFIDFColumn += ") AS sumqfidf";
            columnNames += sumQFIDFColumn;

            //order table by QFIDF score
            SQLiteUtilities.executeSQL(connection, String.Format(@"CREATE TABLE topk AS SELECT {0} FROM allqfidf WHERE {1} ORDER BY sumqfidf DESC", columnNames, ceq));

            //if we get many tuples at first this means we have more than k tuples with the same score(because all found tuples have satisfied all the attribute = queryvalue constraints)
            //so we will apply additional ranking
            if(getCountTopk(connection) >= _k)
            {
                additionalRanking(connection);
                return;
            }


            //now there will come a big nest of if statements
            //but you can follow the controlflow, almost exclusively, from top to bottom
            //i put comments above each if statement to make it easier to follow the control flow

            
            //this is the general controlflow where every step is taken if there are still too little/zero tuples:
            //    -change numerical where clause to WHERE A-4h<=q<=A+4h
            //    -else search just by brand + numerical constraints with margins of 4h
            //    -else search just by model + numerical constraints with margins of 4h
            //    -else search from all cars with margins of 4h
            //    -else select from all cars based on our personal ranking function

            //in case of zero/too little answers
            else
            {
                //change numerical where clause to WHERE A-4*h<=q<=A+4*h
                addMargins(connection, 4);

                //if there are too many tuples after weakening then add additional ranking
                if (getCountTopk(connection) >= _k)
                {
                    additionalRanking(connection);
                    return;
                }

                //if there are still too little tuples, we will increase the margins to 8*h
                else
                {
                    addMargins(connection, 8);
                    //if there are too many tuples after weakening then add additional ranking
                    if (getCountTopk(connection) >= _k)
                    {
                        additionalRanking(connection);
                        return;
                    }

                    //if there are still too little tuples 
                    else
                    {
                        bool queryContainsBrand = false;
                        CategoricalAttribute brand = null;
                        bool queryContainsModel = false;
                        CategoricalAttribute model = null;

                        //check if brand and model are specified in the query
                        foreach (CategoricalAttribute cat in _query.catTerms)
                        {
                            if (cat.attributeName == "brand")
                            {
                                queryContainsBrand = true;
                                brand = cat;
                            }
                            if (cat.attributeName == "model")
                            {
                                queryContainsModel = true;
                            }
                        }

                        //if brand is specified we will search by just brand and numerical constraints with margins of 4*h
                        if (queryContainsBrand)
                        {
                            searchJustByAttributeAndMargins(connection, 4, brand);

                            //if there are too many tuples after weakening then add additional ranking
                            if (getCountTopk(connection) >= _k)
                            {
                                additionalRanking(connection);
                                return;
                            }

                            //else try by brand
                            else if (queryContainsModel)
                            {
                                searchJustByAttributeAndMargins(connection, 4, model);

                                //if there are too many tuples after weakening then add additional ranking
                                if (getCountTopk(connection) >= _k)
                                {
                                    additionalRanking(connection);
                                    return;
                                }
                            }

                            //if model not specified then search all cars with margins of 4*h
                            else
                            {
                                searchJustByMargins(connection, 4);

                                //if there are too many tuples after weakening then add additional ranking
                                if (getCountTopk(connection) >= _k)
                                {
                                    additionalRanking(connection);
                                    return;
                                }
                            }
                        }
                        //case when brand is not specified but model is
                        else if(queryContainsModel)
                        {
                            searchJustByAttributeAndMargins(connection, 4, model);

                            //if there are too many tuples after weakening then add additional ranking
                            if (getCountTopk(connection) >= _k)
                            {
                                additionalRanking(connection);
                                return;
                            }

                            //if still too little tuples
                            else
                            {
                                searchJustByMargins(connection, 4);

                                //if there are too many tuples after weakening then add additional ranking
                                if (getCountTopk(connection) >= _k)
                                {
                                    additionalRanking(connection);
                                    return;
                                }
                                //if still not enough tuples then we search from all cars and rank them with our personal ranking function
                                else
                                {
                                    //get all cars
                                    replaceTopkTable(connection, @"SELECT * FROM allqfidf");

                                    //and rank the topk with our personal function
                                    additionalRanking(connection);
                                    return;
                                }
                            }
                        }

                        //case both brand and model are not specified
                        else
                        {
                            searchJustByMargins(connection, 4);

                            //if there are too many tuples after weakening then add additional ranking
                            if (getCountTopk(connection) >= _k)
                            {
                                additionalRanking(connection);
                                return;
                            }
                            //if still not enough tuples then we search from all cars and rank them with our personal ranking function
                            else
                            {
                                //get all cars
                                replaceTopkTable(connection, @"SELECT * FROM allqfidf");

                                //and rank the topk with our personal function
                                additionalRanking(connection);
                                return;
                            }
                        }

                    }
                }
            }
        }
    }
}
