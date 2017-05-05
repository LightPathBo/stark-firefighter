from mpl_toolkits.basemap import Basemap as Bm
import matplotlib.pyplot as plt
from pyhdf import SD as hdf


class Grafica:
    def __init__(self,lon1,lon2, lat1, lat2):
        if lon2 > lon1:
            self.lon1 = lon1
            self.lon2 = lon2
        else:
            self.lon1 = lon2
            self.lon2 = lon1
        if lat2 > lat1:
            self.lat1 = lat1
            self.lat2 = lat2
        else:
            self.lat1 = lat2
            self.lat2 = lat1
    def plotM(self, name):
        plt.figure()
        m = Bm(llcrnrlon=self.lon1, llcrnrlat=self.lat1, urcrnrlon=self.lon2, urcrnrlat=self.lat2, projection='mill')
        m.shadedrelief()
        plt.savefig(name +'.png',dpi=500)

    def plot(self,datos,name,title):
        plt.figure()
        m = Bm(llcrnrlon=self.lon1, llcrnrlat=self.lat1, urcrnrlon=self.lon2, urcrnrlat=self.lat2, projection='mill')
        m.shadedrelief()
        m.imshow(datos[:])
        m.colorbar()

        #plt.show()
        plt.title(title)
        plt.savefig(name + '.png',dpi=500)

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

class LoadHdf:
    def __init__(self,name):
        self.fileSD = hdf.SD(name)
        self.labels =[]
    def fields(self):
        for k in self.fileSD.datasets().iterkeys():
            tmp = k.lower()
            if 'temperature' in tmp:
                print k
            if 'pressure' in tmp:
                print k
            if 'water_vapor' in tmp:
                print k
            if 'longitude' in tmp:
                print k
            if 'latitude' in tmp:
                print k
    def conf(self,field):
        self.labels.append(field)
    def getData(self, name):
        return self.fileSD.select(name)
    def create(self):
        data = []
        for i in self.labels:
            data.append(self.fileSD.select(i))
        self.testDF = DFbuilder()
        rows = len(data[0][:])
        self.cols = len(data[0][0])
        for i in range(rows):
            for j in range(self.cols):
                for d in data[:-1]:
                    self.testDF.addFeat(float(d[i][j]))
                self.testDF.addLab(float(data[-1][i][j]))

    def __add__(self, other):
        if isinstance(other,LoadHdf):
            dataN = []
            for elem1, elem2 in zip(self.testDF.getDataframe(), other.testDF.getDataframe()):
                dataN.append(elem1 + elem2)
            return dataN

if __name__ == '__main__':
    arch = LoadHdf('datos/MYD06_L2.A2016120.0555.006.2016121022047.hdf')
    archP = LoadHdf('datos/MYD06_L2.A2017120.0605.006.NR.hdf')
    # Available fields
    arch.fields()
    
