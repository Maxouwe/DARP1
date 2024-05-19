using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data.SQLite;

namespace Programma2
{
    class NumericalAttribute
    {
        //h = sigma*n^(-0.2), taken from the paper
        public float h { get; }
        public string attributeName { get; }
        public float queryValue { get; }
        //number of tuples in autompg, used internally for computations
        private int _numTuples;
        public NumericalAttribute(string name, float qval, int numTuples)
        {
            attributeName = name;
            queryValue = qval;
            _numTuples = numTuples;
            //get the h value here, which should be already calculated earlier in preprocessing phase(TO DO)
        }
        
        //creates and fills an QFIDF score table for each attribute, and puts them in the database
        //for each value of attribute A in autompg we create a tuple having a field:
        //           -for that attribute value
        //           -for the qfidf similarity score for that attribute value t and the value specified in the query q
        //where qfidfsimilarity(t, q) = qfsimilarity(t, q) * idfsimilarity(t, q)
        public void createQFIDFTable(SQLiteConnection connection)
        {
            //join the QFSimilarityTable with the IDFSimilarityTable ON QFTable.this.attributeName = IDFTable.this.attributeName
            //and then multiply the columns
        }
        
        public void deleteTables(SQLiteConnection connection)
        {

        }
        //idfsimilarity(t, q) = e^(-0.5 * ((t - q)/h)^2) * IDF(q)
        private void createIDFSimilarityTable()
        {
            //using calcTermIDF as IDF(q), calculate the IDFSimilarityTable
            //SELECT this.attributeName, e^(-0.5 * ((this.attributeName - this.queryValue)/h)^2) * IDF(q) AS RESULT FROM autompg
        }

        //IDF(q) = log(_numTuples/SUM(e^((-0.5*(this.attributeName-this.queryValue)/h)
        private void calcTermIDF()
        {   
            //SELECT log(_numTuples/SUM(e^((-0.5*(this.attributeName-this.queryValue)/h)^2) from autompg
        }
    }

    class CategoricalAttribute
    {
        public string queryValue { get; }
        public string attributeName { get; }
        public CategoricalAttribute(string name, string qval)
        {
            attributeName = name;
            queryValue = qval;
        }

        public void deleteTables(SQLiteConnection connection)
        {
            
        }
    }
}
