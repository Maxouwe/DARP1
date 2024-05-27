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
        public Query _query;
        private int _k;

        public QueryProcessor(Query query, int k)
        {
            _query = query;
            _k = k; 
        }

        public void deleteTables(string connectionString)
        {
            _query.removeTables(connectionString);
            SQLiteUtilities.executeSQL(connectionString, @"DROP TABLE IF EXISTS allqfidf; DROP TABLE IF EXISTS topk;");
        }
        //create a table where all qfidf score tables are joined together with the autompg table
        //they are joined on the autompg.attribute = qfidftable.attribute
        
        private void createQFIDFAnswerTuplesTable(string connectionString)
        {
            _query.createTables(connectionString);

            //join them and select all tuples where all attributes=queryterms
            //they are put into a table called resulttable
            joinTables(connectionString);
        }

        //the code for joining of tables done in createQFIDFAnswerTuplesTable()
        //is put together in this function
        private void joinTables(string connectionString)
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


            SQLiteUtilities.executeSQL(connectionString, String.Format(@"CREATE TABLE allqfidf AS SELECT {0} FROM autompg {1}", columnListString, joinString));
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
        private int getCountTopk(string connectionString)
        {
            int count = 0;
            SQLiteUtilities.readTuples(connectionString, @"SELECT COUNT(*) AS c FROM topk",
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
        private void additionalRanking(string connectionString)
        {
            replaceTopkTable(connectionString, String.Format(@"SELECT *, (horsepower*3 + acceleration*40 + mpg*30 + model_year * 30) AS additionalranking FROM topk ORDER BY horsepower*3 + acceleration*40 + mpg*30 + model_year * 30 DESC LIMIT {0}", _k));
        }

        //instead of searching for tuples where the numerical attributes are exactly equal to the queryvalues
        //we add margins: select where A-m*h =< q =< A + m*h  (A is the attribute, h is the h parameter of that attribute)
        private void addMargins(string connectionString, int m, bool repeated)
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
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf", _query.catTerms[i].attributeName);
                }
                else
                {
                    ceq += String.Format("{0} = {1} AND ", _query.catTerms[i].attributeName, _query.catTerms[i].queryValue);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf + ", _query.catTerms[i].attributeName);
                }
            }

            sumQFIDFColumn += ") AS sumqfidf";
            if(_query.numTerms.Count > 0 || _query.catTerms.Count > 0)
            {
                columnNames += sumQFIDFColumn;
            }

            replaceTopkTable(connectionString, String.Format(@"SELECT {0} FROM allqfidf WHERE {1} ORDER BY sumqfidf DESC LIMIT {2}", columnNames, ceq, _k));

            //if we found enough tuples then we are finished
            if (getCountTopk(connectionString) == _k)
            {
                return;
            }
            //if theres still too little tuples and weve only checked once with margins, then we increase margins
            else if(!repeated)
            {
                addMargins(connectionString, 8, true);
            }
            //if therese still too little tuples and weve already increased the margins we check by brand
            else
            {
                searchAttributeAndMargins(connectionString);
            }
        }
        private void searchAttributeAndMargins(string connectionString)
        {
            bool queryContainsBrand = false;
            CategoricalAttribute brand = null;
            bool queryContainsType = false;
            CategoricalAttribute type = null;

            //check if brand and model are specified in the query
            foreach (CategoricalAttribute cat in _query.catTerms)
            {
                if (cat.attributeName == "brand")
                {
                    queryContainsBrand = true;
                    brand = cat;
                }
                if (cat.attributeName == "type")
                {
                    queryContainsType = true;
                    type = cat;
                }
            }
            //if brand is specified
            if (queryContainsBrand)
            {
                if(_query.numTerms.Count > 0)
                {
                    searchJustByBrandAndMargins(connectionString, brand, type, 4);
                }
                else
                {
                    searchJustByBrand(connectionString, brand, type);
                }
            }
            else if(queryContainsType)
            {
                if(_query.numTerms.Count > 0)
                {
                    searchJustByTypeAndMargins(connectionString, type, 4);
                }
                else
                {
                    searchJustByType(connectionString, type);
                }
            }
            //if neither brand or type are specified then search by just margins
            else if(_query.numTerms.Count > 0)
            {
                searchJustByMargins(connectionString, 4);
            }
            //if there are no numerical specifications then we just return a topk determined by our personal ranking function
            else
            {
                replaceTopkTable(connectionString, @"SELECT * FROM allqfidf");

                //and rank the topk with our personal function
                additionalRanking(connectionString);
                return;
            }
        }
        //search by a single categorical attribute and numerical contraints with margins m*h
        private void searchJustByBrandAndMargins(string connectionString, CategoricalAttribute? brand, CategoricalAttribute? type, int m)
        {
            string columnNames = "allqfidf.id, allqfidf.mpg, allqfidf.cylinders, allqfidf.displacement, allqfidf.horsepower, allqfidf.weight, allqfidf.acceleration, allqfidf.model_year, allqfidf.origin, allqfidf.brand, allqfidf.model, allqfidf.type";
            string ceq = String.Format("{0} = {1} AND ", brand.attributeName, brand.queryValue);
            string sumQFIDFColumn = ", (";

            for (int i = 0; i < _query.numTerms.Count; i++)
            {
                if (i == _query.numTerms.Count - 1)
                {

                    ceq += String.Format("{0} BETWEEN {2}-{3}*{1} and {2}+{3}*{1}", _query.numTerms[i].attributeName, _query.numTerms[i].h, _query.numTerms[i].queryValue, m);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf", _query.numTerms[i].attributeName);
                }
                else
                {
                    ceq += String.Format("{0} BETWEEN {2}-{3}*{1} and {2}+{3}*{1} AND ", _query.numTerms[i].attributeName, _query.numTerms[i].h, _query.numTerms[i].queryValue, m);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf + ", _query.numTerms[i].attributeName);
                }
            }

            sumQFIDFColumn += ") AS sumqfidf";
            columnNames += sumQFIDFColumn;

            replaceTopkTable(connectionString, String.Format(@"SELECT {0} FROM allqfidf WHERE {1} ORDER BY sumqfidf DESC LIMIT {2}", columnNames, ceq, _k));

            if (getCountTopk(connectionString) == _k)
            {
                return;
            }
            //not enought tuples and if type is specified, then try to search by just type
            else if (type != null)
            {
                searchJustByTypeAndMargins(connectionString, type, 4);
            }
            //else try to search by just brand
            else
            {
                searchJustByBrand(connectionString, brand, type);
            }
            
        }
        //search by a single categorical attribute and numerical contraints with margins m*h
        private void searchJustByTypeAndMargins(string connectionString, CategoricalAttribute? type, int m)
        {
            string columnNames = "allqfidf.id, allqfidf.mpg, allqfidf.cylinders, allqfidf.displacement, allqfidf.horsepower, allqfidf.weight, allqfidf.acceleration, allqfidf.model_year, allqfidf.origin, allqfidf.brand, allqfidf.model, allqfidf.type";
            string ceq = String.Format("{0} = {1} AND ", "type", type.queryValue);
            string sumQFIDFColumn = ", (";

            for (int i = 0; i < _query.numTerms.Count; i++)
            {
                if (i == _query.numTerms.Count - 1)
                {

                    ceq += String.Format("{0} BETWEEN {2}-{3}*{1} and {2}+{3}*{1}", _query.numTerms[i].attributeName, _query.numTerms[i].h, _query.numTerms[i].queryValue, m);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf", _query.numTerms[i].attributeName);
                }
                else
                {
                    ceq += String.Format("{0} BETWEEN {2}-{3}*{1} and {2}+{3}*{1} AND ", _query.numTerms[i].attributeName, _query.numTerms[i].h, _query.numTerms[i].queryValue, m);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf + ", _query.numTerms[i].attributeName);
                }
            }

            sumQFIDFColumn += ") AS sumqfidf";
            columnNames += sumQFIDFColumn;

            replaceTopkTable(connectionString, String.Format(@"SELECT {0} FROM allqfidf WHERE {1} ORDER BY sumqfidf DESC LIMIT {2}", columnNames, ceq, _k));

            
            if (getCountTopk(connectionString) == _k)
            {
                return;
            }
            //too little and type is specified? then try to search by just type
            else
            {
                searchJustByType(connectionString, type);
            }
        }
        //search by a single categorical attribute and numerical contraints with margins m*h
        private void searchJustByBrand(string connectionString, CategoricalAttribute? brand, CategoricalAttribute? type)
        {
            string columnNames = "allqfidf.id, allqfidf.mpg, allqfidf.cylinders, allqfidf.displacement, allqfidf.horsepower, allqfidf.weight, allqfidf.acceleration, allqfidf.model_year, allqfidf.origin, allqfidf.brand, allqfidf.model, allqfidf.type";
            string ceq = String.Format("{0} = {1}", "brand", brand.queryValue);

            replaceTopkTable(connectionString, String.Format(@"SELECT {0} FROM allqfidf WHERE {1}", columnNames, ceq));

            //if there are too many tuples for this brand then add additional ranking
            if (getCountTopk(connectionString) >= _k)
            {
                additionalRanking(connectionString);
                return;
            }
            //if type is specified, then try to search by just type
            else if (type != null)
            {
                searchJustByType(connectionString, type);
            }
            //else try to search by just by margins, but we need atleast one specified numerical attribute
            else if (_query.numTerms.Count > 0)
            {
                searchJustByMargins(connectionString, 4);
            }
            //if not then we just search by our personal function
            else
            {
                //get all cars
                replaceTopkTable(connectionString, @"SELECT * FROM allqfidf");

                //and rank the topk with our personal function
                additionalRanking(connectionString);
                return;
            }
        }

        private void searchJustByType(string connectionString, CategoricalAttribute? type)
        {
            string columnNames = "allqfidf.id, allqfidf.mpg, allqfidf.cylinders, allqfidf.displacement, allqfidf.horsepower, allqfidf.weight, allqfidf.acceleration, allqfidf.model_year, allqfidf.origin, allqfidf.brand, allqfidf.model, allqfidf.type";
            string ceq = String.Format("{0} = {1}", "type", type.queryValue);

            replaceTopkTable(connectionString, String.Format(@"SELECT {0} FROM allqfidf WHERE {1}", columnNames, ceq));

            //if there are too many tuples after weakening then add additional ranking
            if (getCountTopk(connectionString) >= _k)
            {
                additionalRanking(connectionString);
                return;
            }
            //if theres still too little tuples try to search by just margins, but there has to be atleast one numerical attribute specified
            else if(_query.numTerms.Count >0)
            {
                searchJustByMargins(connectionString, 4);
            }
            //if not then we just search by our personal function
            else
            {
                //get all cars
                replaceTopkTable(connectionString, @"SELECT * FROM allqfidf");

                //and rank the topk with our personal function
                additionalRanking(connectionString);
                return;
            }
        }
        private void searchJustByMargins(string connectionString, int m)
        {
            string columnNames = "allqfidf.id, allqfidf.mpg, allqfidf.cylinders, allqfidf.displacement, allqfidf.horsepower, allqfidf.weight, allqfidf.acceleration, allqfidf.model_year, allqfidf.origin, allqfidf.brand, allqfidf.model, allqfidf.type";
            string ceq = "";
            string sumQFIDFColumn = ", (";

            for (int i = 0; i < _query.numTerms.Count; i++)
            {
                if (i == _query.numTerms.Count - 1)
                {

                    ceq += String.Format("{0} BETWEEN {2}-{3}*{1} and {2}+{3}*{1}", _query.numTerms[i].attributeName, _query.numTerms[i].h, _query.numTerms[i].queryValue, m);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf", _query.numTerms[i].attributeName);
                }
                else
                {
                    ceq += String.Format("{0} BETWEEN {2}-{3}*{1} and {2}+{3}*{1} AND ", _query.numTerms[i].attributeName, _query.numTerms[i].h, _query.numTerms[i].queryValue, m);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf + ", _query.numTerms[i].attributeName);
                }
            }

            sumQFIDFColumn += ") AS sumqfidf";
            columnNames += sumQFIDFColumn;

            replaceTopkTable(connectionString, String.Format(@"SELECT {0} FROM allqfidf WHERE {1} ORDER BY sumqfidf DESC LIMIT {2}", columnNames, ceq, _k));

            
            if (getCountTopk(connectionString) == _k)
            {
                return;
            }
            //if there not enough tuples then we use our personal function
            else
            {
                //get all cars
                replaceTopkTable(connectionString, @"SELECT * FROM allqfidf");

                //and rank the topk with our personal function
                additionalRanking(connectionString);
                return;
            }
        }

        

        //completely repopulates the topk table
        //based on the given select statement
        private void replaceTopkTable(string connectionString, string selectStatement)
        {
            //create temp table with topk, drop the topk table, create the topk table filled with the new topk, then delete the temporary table
            SQLiteUtilities.executeSQL(connectionString, String.Format(@"CREATE TABLE temp AS {0}; DROP TABLE topk; CREATE TABLE topk AS SELECT * FROM temp; DROP TABLE temp", selectStatement));
        }
         
        public void findTopK(string connectionString)
        {
            //create the autompg table joined with all the qfidf tables
            createQFIDFAnswerTuplesTable(connectionString);

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
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf", _query.catTerms[i].attributeName);
                }
                else
                {
                    ceq += String.Format("{0} = {1} AND ", _query.catTerms[i].attributeName, _query.catTerms[i].queryValue);
                    sumQFIDFColumn += String.Format("allqfidf.{0}qfidf + ", _query.catTerms[i].attributeName);
                }
            }

            sumQFIDFColumn += ") AS sumqfidf";
            if (_query.numTerms.Count > 0 || _query.catTerms.Count > 0)
            {
                columnNames += sumQFIDFColumn;
            }

            //order table by QFIDF score
            SQLiteUtilities.executeSQL(connectionString, String.Format(@"CREATE TABLE topk AS SELECT {0} FROM allqfidf WHERE {1} ORDER BY sumqfidf DESC", columnNames, ceq));

            //if we get many tuples at first this means we have more than k tuples with the same score(because all found tuples have satisfied all the attribute = queryvalue constraints)
            //so we will apply additional ranking
            if(getCountTopk(connectionString) >= _k)
            {
                additionalRanking(connectionString);
                return;
            }


            //in case of zero/too little answers
            //this is the general controlflow where every step is taken if there are still too little/zero tuples:
            //    -change numerical where clause to WHERE A-4h<=q<=A+4h
            //    -change numerical where clause to WHERE A-8h<=q<=A+8h
            //    -else search just by brand with margins of 4h
            //    -else search just by type with margins of 4h
            //    -else search just by margins of 4h
            //    -else search just by brand
            //    -else search just by type
            //    -else select from all cars based on our personal ranking function


            //there is a function for each step, where at the end of each function we check if we found enough tuples
            //with this method, if yes then we return
            //if no then the function leads to the next step to be taken
            //we start with addMargins
            else
            {
                //change numerical where clause to WHERE A-4*h<=q<=A+4*h
                addMargins(connectionString, 4, false);
            }
        }
    }
}
