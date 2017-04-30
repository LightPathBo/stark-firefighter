from pyspark.ml.regression import LinearRegression, GeneralizedLinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.feature import RFormula

from pyhdf import SD as hdf

class DFbuilder:
    def __init__(self):
        self.dataframe = []
        self.tmp =[]
    def addFeat(self,valueF):
        self.tmp.append(valueF)
    def addLab(self,valueL):
        self.tmp.append(valueL)
        self.dataframe.append(self.tmp)
        self.tmp = []
    def addFL(self, valueF, valueL):
        self.dataframe.append([valueF,valueL])
    def getDataframe(self):
        return self.dataframe

if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("FireFigther").getOrCreate()

    fileSD = hdf.SD('datos/MOD11_L2.A2017096.1445.006.2017097093512.hdf')
    emision = fileSD.select('Emis_31')
    lst = fileSD.select('LST')

    testDF = DFbuilder()

    for i in range(4):
        for j in range(len(emision[i])):
            e = emision[i][j]
            l = lst[i][j]
            if e != 0 or l !=0:
               #testDF.addFL(int(e), int(e))
                testDF.addFeat(int(l*0.02))
                testDF.addFeat(int(e))
                testDF.addLab(int(l*0.02))
    print testDF.getDataframe()[:4]

    # Load training data
    test = spark.createDataFrame(testDF.getDataframe(),
        ['T','E','TE']) # Temperatura, Emisividad, TemperaturaPosterior

    formula = RFormula(
        formula="TE ~ T + E",
        featuresCol="features",
        labelCol="label")
    output = formula.fit(test).transform(test)
    #output.select("features", "label").show()
    output.show()

    glr = GeneralizedLinearRegression(family="gaussian", link="identity", maxIter=10, regParam=0.3)

    # Fit the model
    gModel = glr.fit(output)

    result = gModel.transform(output)
    result.show()
