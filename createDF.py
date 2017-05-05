from pyspark.ml.regression import LinearRegression, GeneralizedLinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.feature import RFormula

from pyhdf import SD as hdf
from simVhd import DFbuilder, Grafica, LoadHdf

class ExportHdf:
    pass

if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("FireFigther").getOrCreate()

    fileSD = hdf.SD('datos/MYD06_L2.A2016120.0555.006.2016121022047.hdf')
    fileSDP = hdf.SD('datos/MYD06_L2.A2017120.0605.006.NR.hdf')

    arch = LoadHdf('datos/MYD06_L2.A2016120.0555.006.2016121022047.hdf')
    arch.conf('Longitude')
    arch.conf('Latitude')
    # Some conditions
    arch.conf('Surface_Temperature')
    arch.conf('Cloud_Effective_Emissivity')
    arch.create()

    archP = LoadHdf('datos/MYD06_L2.A2017120.0605.006.NR.hdf')
    archP.conf('Surface_Temperature')
    archP.create()
    #----------------------

    lat = arch.getData('Latitude')
    lon = arch.getData('Longitude')
    latM = min(map(lambda x: min(x), lat[:]))
    latX = max(map(lambda x: max(x), lat[:]))
    lonM = min(map(lambda x: min(x), lon[:]))
    lonX = max(map(lambda x: max(x), lon[:]))

    # Load training data
    data = spark.createDataFrame((arch + archP), arch.labels + ['Surface_TemperatureP'])
    train, test =data.randomSplit([0.6, 0.4])
    formula = RFormula(formula="Surface_TemperatureP ~ Surface_Temperature + Cloud_Effective_Emissivity", featuresCol="features", labelCol="label")
    #outputTrain = formula.fit(train).transform(train)
    #outputTest = formula.fit(test).transform(test)
    output = formula.fit(data).transform(data)

    glr = GeneralizedLinearRegression(family="gaussian", link="identity", maxIter=10, regParam=0.3)
    gModel = glr.fit(output)
    result = gModel.transform(output)
    result.show()

    prev = map(lambda x: int(x[0]), result.select('prediction').collect())

    lin = arch.cols
    final=[]

    for i in range(len(prev)/lin):
        tmp = prev[i*lin:(i+1)*lin]
        if len(tmp)!=0:
            final.append(tmp)

    summary = gModel.summary
    print("Dispersion: " + str(summary.dispersion))

    test = Grafica(lonM, lonX, latM, latX)
    test.plotM('coordenadas')
    test.plot(arch.getData('Surface_Temperature'), 'tempI', 'Surface_Temperature_Initial')
    test.plot(arch.getData('Cloud_Effective_Emissivity'), 'emiI', 'Cloud_Effective_Emissivity_Initial')
    test.plot(archP.getData('Surface_Temperature'), 'TEMPR', 'Surface_Temperature_Final')
    test.plot(final, 'TEMPP', 'Surface_Temperature_Prediction')
    
