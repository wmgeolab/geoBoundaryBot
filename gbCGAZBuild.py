import json
import pandas as pd
import sys
import subprocess
import os

outPath = "/home/dan/tmp/CGAZ/"
gBPath = "/home/dan/git/geoBoundaries/releaseData/gbOpen/"

stdGeom = "./dta/usDoSLSIB_Mar2020.geojson"
stdISO= "./dta/iso_3166_1_alpha_3.csv"

with open(stdGeom) as f:
    globalDta = json.load(f)

isoCSV = pd.read_csv(stdISO)

disputedGeoms = {}
disputedGeoms['type'] = globalDta['type']
disputedGeoms['crs'] = globalDta['crs']
disputedGeoms['features'] = []

otherGeoms = {}
otherGeoms['type'] = globalDta['type']
otherGeoms['crs'] = globalDta['crs']
otherGeoms['features'] = []

#Iterate over the DoS cases
for i in range(0, len(globalDta['features'])):
    #Handle disputed regions.
    #All disputed regions will be assigned to a "Disputed" set of regions, burned in at the end.
    if ("(disp)" in str(globalDta['features'][i]['properties']['COUNTRY_NA'])):
        disputedGeoms['features'].append(globalDta['features'][i])

    else:
        #For CGAZ, all territories are merged into their parent country.
        if("(UK)" in globalDta['features'][i]['properties']['COUNTRY_NA']):
            globalDta['features'][i]['properties']['COUNTRY_NA'] = "United Kingdom"

        if("(US)" in globalDta['features'][i]['properties']['COUNTRY_NA']):
            globalDta['features'][i]['properties']['COUNTRY_NA'] = "United States"

        if("(Aus)" in globalDta['features'][i]['properties']['COUNTRY_NA']):
            globalDta['features'][i]['properties']['COUNTRY_NA'] = "Australia"

        if("Greenland (Den)" in globalDta['features'][i]['properties']['COUNTRY_NA']):
            globalDta['features'][i]['properties']['COUNTRY_NA'] = "Greenland"

        if("(Den)" in globalDta['features'][i]['properties']['COUNTRY_NA']):
            globalDta['features'][i]['properties']['COUNTRY_NA'] = "Denmark"

        if("(Fr)" in globalDta['features'][i]['properties']['COUNTRY_NA']):
            globalDta['features'][i]['properties']['COUNTRY_NA'] = "France"

        if("(Ch)" in globalDta['features'][i]['properties']['COUNTRY_NA']):
            globalDta['features'][i]['properties']['COUNTRY_NA'] = "China"

        if("(Nor)" in globalDta['features'][i]['properties']['COUNTRY_NA']):
            globalDta['features'][i]['properties']['COUNTRY_NA'] = "Norway"

        if("(NZ)" in globalDta['features'][i]['properties']['COUNTRY_NA']):
            globalDta['features'][i]['properties']['COUNTRY_NA'] = "New Zealand"

        if("Netherlands [Caribbean]" in globalDta['features'][i]['properties']['COUNTRY_NA']):
            globalDta['features'][i]['properties']['COUNTRY_NA'] = "Netherlands"

        if("(Neth)" in globalDta['features'][i]['properties']['COUNTRY_NA']):
            globalDta['features'][i]['properties']['COUNTRY_NA'] = "Netherlands"

        if("Portugal [" in globalDta['features'][i]['properties']['COUNTRY_NA']):
            globalDta['features'][i]['properties']['COUNTRY_NA'] = "Portugal"

        if("Spain [" in globalDta['features'][i]['properties']['COUNTRY_NA']):
            globalDta['features'][i]['properties']['COUNTRY_NA'] = "Spain"   

        otherGeoms['features'].append(globalDta['features'][i])

#Add ISO codes

#This function is ridiculous, and slowly grew by accretion.
#Need to just do a list at some point.
#Don't want to change the underlying data is the challenge.
def isoLookup(country):
    if(country == "Antigua & Barbuda"):
        return("ATG")
    if(country == "Bahamas, The"):
        return("BHS")
    if(country == "Bosnia & Herzegovina"):
        return("BIH")
    if(country == "Congo, Dem Rep of the"):
        return("COD")
    if(country == "Congo, Rep of the"):
        return("COG")
    if(country == "Cabo Verde"):
        return("CPV")
    if(country == "Cote d'Ivoire"):
        return("CIV")
    if(country == "Central African Rep"):
        return("CAF")
    if(country == "Czechia"):
        return("CZE")
    if(country == "Gambia, The"):
        return("GMB")
    if(country == "Iran"):
        return("IRN")
    if(country == "Korea, North"):
        return("PRK")
    if(country == "Korea, South"):
        return("KOR")
    if(country == "Laos"):
        return("LAO")
    if(country == "Macedonia"):
        return("MKD")
    if(country == "Marshall Is"):
        return("MHL")
    if(country == "Micronesia, Fed States of"):
        return("FSM")
    if(country == "Moldova"):
        return("MDA")
    if(country == "Sao Tome & Principe"):
        return("STP")
    if(country == "Solomon Is"):
        return("SLB")
    if(country == "St Kitts & Nevis"):
        return("KNA")
    if(country == "St Lucia"):
        return("LCA")
    if(country == "St Vincent & the Grenadines"):
        return("VCT")
    if(country == "Syria"):
        return("SYR")
    if(country == "Tanzania"):
        return("TZA")
    if(country == "Vatican City"):
        return("VAT")
    if(country == "United States"):
        return("USA")
    if(country == "Antarctica"):
        return("ATA")
    if(country == "Bolivia"):
        return("BOL")
    if(country == "Brunei"):
        return("BRN")
    if(country == "Russia"):
        return("RUS")
    if(country == "Trinidad & Tobago"):
        return("TTO")
    if(country == "Swaziland"):
        return("SWZ")
    if(country == "Venezuela"):
        return("VEN")
    if(country == "Vietnam"):
        return("VNM")
    if(country == "Burma"):
        return("MMR")
    return("No Match")

for i in range(0, len(otherGeoms['features'])):
    cName = otherGeoms['features'][i]['properties']['COUNTRY_NA']
    match = isoCSV[isoCSV['Name'] == cName]
    if(len(match) == 1):
        print(match.reset_index()['Alpha-3code'][0])
        otherGeoms['features'][i]['properties']['ISO_CODE'] = match.reset_index()['Alpha-3code'][0]
    else:
        if(isoLookup(cName) != "No Match"):
            otherGeoms['features'][i]['properties']['ISO_CODE'] = isoLookup(cName)
        else:
            print("Error - no match.")
            print(cName)
            sys.exit()

for i in range(0, len(disputedGeoms['features'])):
    cName = disputedGeoms['features'][i]['properties']['COUNTRY_NA'].replace(" (disp)", "")
    match = isoCSV[isoCSV['Name'] == cName]
    if(len(match) == 1):
        disputedGeoms['features'][i]['properties']['ISO_CODE'] = match.reset_index()['Alpha-3code'][0]
    else:
        if(isoLookup(cName) != "No Match"):
            disputedGeoms['features'][i]['properties']['ISO_CODE'] = isoLookup(cName)
        else:
            disputedGeoms['features'][i]['properties']['ISO_CODE'] = "None"

#Save the two geometries
with open(outPath + "baseISO.geojson", "w") as f:
    json.dump(otherGeoms, f)

with open(outPath + "disputedISO.geojson", "w") as f:
    json.dump(disputedGeoms, f)


#Dissolve based on ISO Code
msRun = ("mapshaper-xl " + outPath + "baseISO.geojson" + 
        " -dissolve fields='ISO_CODE' multipart"  +
        " -o force format=geojson " + outPath + "baseISO.geojson")
process = subprocess.Popen(
                [msRun],
                shell=True, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE)
process.wait()
output, error = process.communicate()
print(str(error) + msRun)

msRun = ("mapshaper-xl " + outPath + "disputedISO.geojson" + 
        " -dissolve fields='ISO_CODE' multipart" +
        " -o format=geojson " + outPath + "disputedISO.geojson")
process = subprocess.Popen(
                [msRun],
                shell=True, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE)
process.wait()
output, error = process.communicate()
print(str(error) + msRun)

#reload with the dissolve applied
with open(outPath + "baseISO.geojson") as f:
    otherGeoms = json.load(f)

#Iterate over all regions in the baseISO.
CGAZ_ADM0 = {}
CGAZ_ADM0['type'] = globalDta['type']
CGAZ_ADM0['crs'] = globalDta['crs']
CGAZ_ADM0['features'] = []

CGAZ_ADM1 = {}
CGAZ_ADM1['type'] = globalDta['type']
CGAZ_ADM1['crs'] = globalDta['crs']
CGAZ_ADM1['features'] = []

CGAZ_ADM2 = {}
CGAZ_ADM2['type'] = globalDta['type']
CGAZ_ADM2['crs'] = globalDta['crs']
CGAZ_ADM2['features'] = []

adm0str = ""
adm1str = ""
adm2str = ""

for i in range(0, len(otherGeoms['features'])):
    A0 = {}
    A0['type'] = globalDta['type']
    A0['crs'] = globalDta['crs']
    A0['features'] = []

    A1 = {}
    A1['type'] = globalDta['type']
    A1['crs'] = globalDta['crs']
    A1['features'] = []

    A2 = {}
    A2['type'] = globalDta['type']
    A2['crs'] = globalDta['crs']
    A2['features'] = []

    curISO = otherGeoms['features'][i]['properties']['ISO_CODE']

    A0Path = outPath + "ADM0_"+curISO+".geojson"
    adm0str = adm0str + A0Path + " "
    
    #ADM0 we're working on
    A0["features"].append(otherGeoms['features'][i])

    with open(A0Path, "w") as f:
        json.dump(A0, f)

    #Get the ADM1 and ADM2 from geoBoundaries.
    #There should be only RARE exceptions where ADM1 and ADM2 do not exist.
    A1Path = gBPath + curISO + "/ADM1/geoBoundaries-"+curISO+"-ADM1.geojson"
    A2Path = gBPath + curISO + "/ADM2/geoBoundaries-"+curISO+"-ADM2.geojson"

    if(os.path.isfile(A1Path)):
        msRun = ("mapshaper-xl " + A1Path + 
                " -simplify keep-shapes percentage=0.10 "
                " -clip " + A0Path + 
                " -o format=topojson " + outPath + "ADM1_" + curISO + ".topojson")
        process = subprocess.Popen(
                        [msRun],
                        shell=True, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE)
        process.wait()
        output, error = process.communicate()
        print(str(error) + msRun)

        adm1str = adm1str + outPath + "ADM1_" + curISO + ".topojson "
    
    if(os.path.isfile(A2Path)):
        msRun = ("mapshaper-xl " + A2Path + 
                " -simplify keep-shapes percentage=0.10 "
                " -clip " + A0Path + 
                " -o format=topojson " + outPath + "ADM2_" + curISO + ".topojson")
        process = subprocess.Popen(
                        [msRun],
                        shell=True, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE)
        process.wait()
        output, error = process.communicate()
        print(str(error) + msRun)

        adm2str = adm2str + outPath + "ADM2_" + curISO + ".topojson "

#Join the ADM0 / ADM1 / ADM2s together into one large geom.
dropFields = "PROV_34_NA,DIST_34_NA,OBJECTID,Shape_Leng,Shape_Area,shapeISO,id,OBJECTID,"

A0mapShaperFull = ("mapshaper-xl -i " + adm0str + " " + outPath + "disputedISO.geojson" +
                       " combine-files -merge-layers force" +
                       " name=globalADM0" +
                       #" -simplify weighted " + ratio + "% keep-shapes" +
                       " -clean gap-fill-area=10000km2 keep-shapes" +
                       " -drop fields=" + dropFields +
                       " -o format=topojson " + (outPath + "geoBoundariesCGAZ_ADM0.topojson") +
                       " -o format=geojson " + (outPath + "geoBoundariesCGAZ_ADM0.geojson") +
                       " -o format=shapefile " + (outPath + "geoBoundariesCGAZ_ADM0.shp")       
                      )
process = subprocess.Popen(
                    [A0mapShaperFull],
                    shell=True, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE)
process.wait()
output, error = process.communicate()
print(str(error) + msRun)





A1mapShaperFull = ("mapshaper-xl -i " + adm1str + " " + outPath + "disputedISO.geojson" +
                       " combine-files -merge-layers force" +
                       " name=globalADM1" +
                       #" -simplify weighted " + ratio + "% keep-shapes" +
                       " -clean gap-fill-area=10000km2 keep-shapes" +
                       " -drop fields=" + dropFields +
                       " -o format=topojson " + (outPath + "geoBoundariesCGAZ_ADM1.topojson") +
                       " -o format=geojson " + (outPath + "geoBoundariesCGAZ_ADM1.geojson") +
                       " -o format=shapefile " + (outPath + "geoBoundariesCGAZ_ADM1.shp")       
                      )
process = subprocess.Popen(
                    [A1mapShaperFull],
                    shell=True, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE)
process.wait()
output, error = process.communicate()
print(str(error) + msRun)

A2mapShaperFull = ("mapshaper-xl -i " + adm2str + " " + outPath + "disputedISO.geojson" +
                       " combine-files -merge-layers force" +
                       " name=globalADM2" +
                       #" -simplify weighted " + ratio + "% keep-shapes" +
                       " -clean gap-fill-area=10000km2 keep-shapes" +
                       " -drop fields=" + dropFields +
                       " -o format=topojson " + (outPath + "geoBoundariesCGAZ_ADM2.topojson") +
                       " -o format=geojson " + (outPath + "geoBoundariesCGAZ_ADM2.geojson") +
                       " -o format=shapefile " + (outPath + "geoBoundariesCGAZ_ADM2.shp")       
                      )
process = subprocess.Popen(
                    [A2mapShaperFull],
                    shell=True, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE)
process.wait()
output, error = process.communicate()
print(str(error) + msRun)