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
    def plotM(self):
        #m = Bm(llcrnrlon=lon[i][0], llcrnrlat=lat[i][-1], urcrnrlon=lon[i][-1], urcrnrlat=lat[i][0], projection='mill')
        m = Bm(llcrnrlon=self.lon1, llcrnrlat=self.lat1, urcrnrlon=self.lon2, urcrnrlat=self.lat2, projection='mill')
        m.shadedrelief()
        plt.savefig('mapa.png',dpi=500)

    def plot(self,datos):
        #m = Bm(llcrnrlon=lon[i][0], llcrnrlat=lat[i][-1], urcrnrlon=lon[i][-1], urcrnrlat=lat[i][0], projection='mill')
        m = Bm(llcrnrlon=self.lon1, llcrnrlat=self.lat1, urcrnrlon=self.lon2, urcrnrlat=self.lat2, projection='mill')
        m.shadedrelief()
        m.imshow(datos[:])
        m.colorbar()

        #plt.show()
        plt.savefig('mapaT.png',dpi=500)



if __name__ == '__main__':

    # fileSD = hdf.SD('/home/dextron/PycharmProjects/incendioHack/datos/MOD11_L2.A2017119.0045.006.NRT.hdf')
    fileSD = hdf.SD('datos/MOD11_L2.A2017096.1445.006.2017097093512.hdf')
    
    lat = fileSD.select('Latitude')
    lon = fileSD.select('Longitude')
    emision = fileSD.select('Emis_31')
    #emision2 = fileSD.select('Emis_32')
    lsData = fileSD.select('LST')
    print lsData.attributes()
    print lsData.getdatastrs()
    print emision.attributes()
    print emision.getdatastrs()
    print emision.info()

    latM = min(map(lambda x : min(x), lat[:]))
    latX = max(map(lambda x : max(x), lat[:]))
    lonM = min(map(lambda x: min(x), lon[:]))
    lonX = max(map(lambda x: max(x), lon[:]))

    #Test grafica
    test = Grafica(lonM, lonX, latM, latX)
    test.plotM()    # mapa coordenadas
    test.plot(lsData)   # mapa de temperaturas escalado
    #test.plot(emision2)

