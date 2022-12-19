import logging
import os
import sys
import warnings
from subprocess import PIPE, run
import geopandas
import pandas as pd
import shutil

GB_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries/"
LOG_DIR = "/sciclone/geograd/geoBoundaries/logs/gbCGAZ/"
BOT_DIR = "/sciclone/geograd/geoBoundaries/scripts/geoBoundaryBot/"
TMP_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbCGAZ/"



# ignore warnings about using '()' in str.contains https://stackoverflow.com/a/39902267/697964
warnings.filterwarnings("ignore", "This pattern has match groups")

class cgazBuilder():
    def __init__(self, GB_DIR, LOG_DIR, BOT_DIR, TMP_DIR):
        self.stdGeom = BOT_DIR + "dta/usDoSLSIB_Mar2020.geojson"
        self.stdISO = BOT_DIR + "dta/iso_3166_1_alpha_3.csv"
        self.GB_DIR = GB_DIR
        self.LOG_DIR = LOG_DIR
        self.BOT_DIR = BOT_DIR
        self.TMP_DIR = TMP_DIR
        self.log = logging.getLogger()

    def cmd(self, command, **kwargs):
        r = run(command, universal_newlines=True, shell=True, **kwargs)
        self.log.debug(r.args)
        if r.returncode != 0:
            self.log.error("Process had a non-0 return code: {r.returncode}")
        return r

    def preprocess_dta(self):
        globalDta = geopandas.read_file(self.stdGeom)
        self.isoCSV = pd.read_csv(self.stdISO)

        # Separate disputedG regions.
        # All disputedG regions will be assigned to a "Disputed" set of regions, burned in at the end.
        self.disputedG = globalDta[globalDta["COUNTRY_NA"].str.contains("(disp)")].copy()
        self.G = globalDta[~globalDta["COUNTRY_NA"].str.contains("(disp)")].copy()

        # For CGAZ, all territories are merged into their parent country.
        # Cleanup country names in DoS cases
        def country_renamer(country_na: str):
            test_dict = {
                "(US)": "United States",
                "(UK)": "United Kingdom",
                "(Aus)": "Australia",
                "Greenland (Den)": "Greenland",
                "(Den)": "Denmark",
                "(Fr)": "France",
                "(Ch)": "China",
                "(Nor)": "Norway",
                "(NZ)": "New Zealand",
                "Netherlands [Caribbean]": "Netherlands",
                "(Neth)": "Netherlands",
                "Portugal [": "Portugal",
                "Spain [": "Spain",
            }

            default = [country_na]
            country_na = [v for k, v in test_dict.items() if k in country_na] + default
            return country_na[0]

        self.G.COUNTRY_NA = self.G.COUNTRY_NA.map(country_renamer)
        self.G["ISO_CODE"] = self.G.COUNTRY_NA.map(self.isoLookup)

        # check for nulls in ISO_CODE
        features_without_iso_code = self.G[self.G.ISO_CODE.isna() | self.G.ISO_CODE == ""]
        if len(features_without_iso_code) > 0:
            print("Error - no match.")
            print(features_without_iso_code)
            sys.exit(1)

        self.disputedG.COUNTRY_NA = self.disputedG.COUNTRY_NA.str.replace(" (disp)", "", regex=False)
        self.disputedG["ISO_CODE"] = self.disputedG.COUNTRY_NA.map(self.isoLookup)
        self.disputedG[self.disputedG.ISO_CODE.isna() | self.disputedG.ISO_CODE == ""].ISO_CODE = "None"

        self.G.to_file(TMP_DIR + "baseISO.geojson", driver="GeoJSON")
        self.disputedG.to_file(TMP_DIR + "disputedISO.geojson", driver="GeoJSON")

    def isoLookup(self,country):
        switcher = {
            "Antigua & Barbuda": "ATG",
            "Bahamas, The": "BHS",
            "Bosnia & Herzegovina": "BIH",
            "Congo, Dem Rep of the": "COD",
            "Congo, Rep of the": "COG",
            "Cabo Verde": "CPV",
            "Cote d'Ivoire": "CIV",
            "Central African Rep": "CAF",
            "Czechia": "CZE",
            "Gambia, The": "GMB",
            "Iran": "IRN",
            "Korea, North": "PRK",
            "Korea, South": "KOR",
            "Laos": "LAO",
            "Macedonia": "MKD",
            "Marshall Is": "MHL",
            "Micronesia, Fed States of": "FSM",
            "Moldova": "MDA",
            "Sao Tome & Principe": "STP",
            "Solomon Is": "SLB",
            "St Kitts & Nevis": "KNA",
            "St Lucia": "LCA",
            "St Vincent & the Grenadines": "VCT",
            "Syria": "SYR",
            "Tanzania": "TZA",
            "Vatican City": "VAT",
            "United States": "USA",
            "Antarctica": "ATA",
            "Bolivia": "BOL",
            "Brunei": "BRN",
            "Russia": "RUS",
            "Trinidad & Tobago": "TTO",
            "Swaziland": "SWZ",
            "Venezuela": "VEN",
            "Vietnam": "VNM",
            "Burma": "MMR",
        }
        switcher_match = switcher.get(country, None)

        isoCSV_match = self.isoCSV[self.isoCSV["Name"] == country]
        if len(isoCSV_match) == 1:
            isoCSV_match = self.isoCSV[self.isoCSV["Name"] == country]["Alpha-3code"].values[0]
            self.log.debug(f"csv {isoCSV_match} {country} {isoCSV_match}")
            return isoCSV_match

        self.log.debug(f"swi {switcher_match} {country}")
        return switcher_match

    def process_geometries(self):
        # reload with the dissolve applied
        LG = geopandas.read_file(TMP_DIR + "baseISO.geojson")

        adm0str = ""
        adm1str = ""
        adm2str = ""
        for i in LG.index:
            g = LG.loc[[i]]
            adm0str, adm1str, adm2str = self.process_geometry(g, adm0str, adm1str, adm2str)

        return adm0str, adm1str, adm2str

    def process_geometry(self, g, adm0str, adm1str, adm2str):
        curISO = g["ISO_CODE"].values[0]
        g["shapeGroup"] = g["ISO_CODE"]
        g["shapeType"] = "ADM0"
        DTA_A0Path = self.TMP_DIR + "ADM0_" + curISO + ".geojson"
        g.to_file(DTA_A0Path, driver="GeoJSON")

        # Get the ADM1 and ADM2 from geoBoundaries.
        # There should be only RARE exceptions where ADM1 and ADM2 do not exist.
        A0Path = self.GB_DIR + "releaseData/gbOpen/" + curISO + "/ADM0/geoBoundaries-" + curISO + "-ADM0.geojson"
        A1Path = self.GB_DIR + "releaseData/gbOpen/" + curISO + "/ADM1/geoBoundaries-" + curISO + "-ADM1.geojson"
        A2Path = self.GB_DIR + "releaseData/gbOpen/" + curISO + "/ADM2/geoBoundaries-" + curISO + "-ADM2.geojson"
        # If they do not exist use the nearest layer to not have gaps in the world
        if not os.path.isfile(A1Path):
            A1Path = A0Path
        if not os.path.isfile(A2Path):
            A2Path = A1Path

        adm1out = self.TMP_DIR + "ADM1_" + curISO + ".topojson"
        adm2out = self.TMP_DIR + "ADM2_" + curISO + ".topojson"


        adm1cmd = self.cmd("mapshaper-xl 24gb " + A1Path + " -simplify keep-shapes percentage=0.10 -clip " + DTA_A0Path + " -o format=topojson " + adm1out)
        print(adm1cmd)


        adm2cmd = self.cmd("mapshaper-xl 24gb " + A2Path + " -simplify keep-shapes percentage=0.10 -clip " + DTA_A0Path + " -o format=topojson " + adm2out)
        print(adm2cmd)

        adm0str = adm0str + DTA_A0Path + " "
        adm1str = adm1str + self.TMP_DIR + "ADM1_" + curISO + ".topojson "
        adm2str = adm2str + self.TMP_DIR + "ADM2_" + curISO + ".topojson "

        return adm0str, adm1str, adm2str

    def dissolve_based_on_ISO_Code(self):
        self.cmd(f"mapshaper-xl 24gb " + self.TMP_DIR + "baseISO.geojson -dissolve fields='ISO_CODE' multipart -o force format=geojson "+self.TMP_DIR+"baseISO.geojson")
        self.cmd(f"mapshaper-xl 24gb " + self.TMP_DIR + "disputedISO.geojson -dissolve fields='ISO_CODE' multipart -o force format=geojson "+self.TMP_DIR+"disputedISO.geojson")

    def join_admins(self,adm0str, adm1str, adm2str):
    # Join the ADM0 / ADM1 / ADM2s together into one large geom.
        dropFields = "FID,PROV_34_NA,DIST_34_NA,OBJECTID,Shape_Leng,Shape_Area,shapeISO,id,OBJECTID,id,COUNTRY_NA,Shape_Le_1,ADM1_NAME,admin1Name,Type,'ISO Code',LEVEL,LEVEL_1,Shape_Length,ISO2,LEVEL2,SHAPE_Leng,SHAPE_Area,DISTRICT,admin2Name,OBJECTID_1,SOVEREIGNT,Level,MAX_Name,MAX_ISO_Co,MAX_Level,ISO_CODE,ISO_CODE2"

        self.cgazOutPath = self.GB_DIR + "releaseData/CGAZ/"
        #Remove any existing files
        try:
            shutil.rmtree(self.cgazOutPath)
        except:
            print("No previous build to delete.")
        
        os.mkdir(self.cgazOutPath)


        A0mapShaperFull = (
            "mapshaper-xl 24gb -i "
            + adm0str
            + " "
            + self.TMP_DIR
            + "disputedISO.geojson"
            + " combine-files -merge-layers force"
            + " name=globalADM0"
            +
            # " -simplify weighted " + ratio + "% keep-shapes" +
            " -clean gap-fill-area=10000km2 keep-shapes"
            + " -drop fields="
            + dropFields
            + " -o format=topojson "
            + (self.cgazOutPath + "geoBoundariesCGAZ_ADM0.topojson")
            + " -o format=geojson "
            + (self.cgazOutPath + "geoBoundariesCGAZ_ADM0.geojson")
            + " -o format=shapefile "
            + (self.cgazOutPath + "geoBoundariesCGAZ_ADM0.shp")
        )
        A1mapShaperFull = (
            "mapshaper-xl 24gb -i "
            + adm1str
            + " "
            + self.TMP_DIR
            + "disputedISO.geojson"
            + " combine-files -merge-layers force"
            + " name=globalADM1"
            +
            # " -simplify weighted " + ratio + "% keep-shapes" +
            " -clean gap-fill-area=10000km2 keep-shapes"
            + " -drop fields="
            + dropFields
            + " -o format=topojson "
            + (self.cgazOutPath + "geoBoundariesCGAZ_ADM1.topojson")
            + " -o format=geojson "
            + (self.cgazOutPath + "geoBoundariesCGAZ_ADM1.geojson")
            + " -o format=shapefile "
            + (self.cgazOutPath + "geoBoundariesCGAZ_ADM1.shp")
        )
        A2mapShaperFull = (
            "mapshaper-xl 24gb -i "
            + adm2str
            + " "
            + self.TMP_DIR
            + "disputedISO.geojson"
            + " combine-files -merge-layers force"
            + " name=globalADM2"
            +
            # " -simplify weighted " + ratio + "% keep-shapes" +
            " -clean gap-fill-area=10000km2 keep-shapes"
            + " -drop fields="
            + dropFields
            + " -o format=topojson "
            + (self.cgazOutPath + "geoBoundariesCGAZ_ADM2.topojson")
            + " -o format=geojson "
            + (self.cgazOutPath + "geoBoundariesCGAZ_ADM2.geojson")
            + " -o format=shapefile "
            + (self.cgazOutPath + "geoBoundariesCGAZ_ADM2.shp")
        )

        self.cmd(A0mapShaperFull)
        self.cmd(A1mapShaperFull)
        self.cmd(A2mapShaperFull)

        self.cmd("ogr2ogr " + self.cgazOutPath + "geoBoundariesCGAZ_ADM0.gpkg " + self.cgazOutPath + "geoBoundariesCGAZ_ADM0.topojson")
        self.cmd("ogr2ogr " + self.cgazOutPath + "geoBoundariesCGAZ_ADM1.gpkg " + self.cgazOutPath + "geoBoundariesCGAZ_ADM1.topojson")
        self.cmd("ogr2ogr " + self.cgazOutPath + "geoBoundariesCGAZ_ADM2.gpkg " + self.cgazOutPath + "geoBoundariesCGAZ_ADM2.topojson")

build = cgazBuilder(GB_DIR = GB_DIR, LOG_DIR = LOG_DIR, BOT_DIR = BOT_DIR, TMP_DIR = TMP_DIR)
print(build.preprocess_dta())
print(build.dissolve_based_on_ISO_Code())
adm0str, adm1str, adm2str = build.process_geometries()
print(build.join_admins(adm0str, adm1str, adm2str))


