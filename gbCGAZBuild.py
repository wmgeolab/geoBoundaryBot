import logging
import os
import sys
import warnings
from subprocess import PIPE, run

import geopandas
import pandas as pd
from rich import inspect
from rich.logging import RichHandler
from rich.traceback import install

install()

outPath = "tmp/CGAZ/"
gBPath = "../geoBoundaries/releaseData/gbOpen/"

stdGeom = "./dta/usDoSLSIB_Mar2020.geojson"
stdISO = "./dta/iso_3166_1_alpha_3.csv"

# ignore warnings about using '()' in str.contains https://stackoverflow.com/a/39902267/697964
warnings.filterwarnings("ignore", "This pattern has match groups")


def cmd(command, **kwargs):
    log = logging.getLogger()
    r = run(
        command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True, **kwargs
    )
    log.debug(r.args)
    if r.returncode != 0:
        log.error(f"process exited with returncode {r.returncode}")
    log.info(r.stdout.strip())
    log.error(r.stderr.strip())
    return r


def argparse_log(args):
    print(args)
    log_levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    logging.basicConfig(
        level=log_levels[min(len(log_levels) - 1, args.verbose)],
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler()],
    )
    log = logging.getLogger()
    return log


def preprocess_dta(log):
    globalDta = geopandas.read_file(stdGeom)
    isoCSV = pd.read_csv(stdISO)

    # Separate disputedG regions.
    # All disputedG regions will be assigned to a "Disputed" set of regions, burned in at the end.
    disputedG = globalDta[globalDta["COUNTRY_NA"].str.contains("(disp)")].copy()
    G = globalDta[~globalDta["COUNTRY_NA"].str.contains("(disp)")].copy()

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

    G.COUNTRY_NA = G.COUNTRY_NA.map(country_renamer)

    # Add ISO codes

    # Need to just do a list at some point.
    # Don't want to change the underlying data is the challenge.
    def isoLookup(country):
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

        isoCSV_match = isoCSV[isoCSV["Name"] == country]
        if len(isoCSV_match) == 1:
            isoCSV_match = isoCSV[isoCSV["Name"] == country]["Alpha-3code"].values[0]
            log.debug(f"csv {isoCSV_match} {country} {isoCSV_match}")
            return isoCSV_match

        log.debug(f"swi {switcher_match} {country}")
        return switcher_match

    G["ISO_CODE"] = G.COUNTRY_NA.map(isoLookup)

    # check for nulls in ISO_CODE
    features_without_iso_code = G[G.ISO_CODE.isna() | G.ISO_CODE == ""]
    if len(features_without_iso_code) > 0:
        print("Error - no match.")
        print(features_without_iso_code)
        sys.exit(1)

    disputedG.COUNTRY_NA = disputedG.COUNTRY_NA.str.replace(" (disp)", "", regex=False)
    disputedG["ISO_CODE"] = disputedG.COUNTRY_NA.map(isoLookup)
    disputedG[disputedG.ISO_CODE.isna() | disputedG.ISO_CODE == ""].ISO_CODE = "None"

    G.to_file("./baseISO.geojson", driver="GeoJSON")
    disputedG.to_file("./disputedISO.geojson", driver="GeoJSON")


def process_geometries(log, args):
    # reload with the dissolve applied
    G = geopandas.read_file(f"{outPath}baseISO.geojson")

    adm0str = ""
    adm1str = ""
    adm2str = ""
    for i in G.index:
        g = G.loc[[i]]
        adm0str, adm1str, adm2str = process_geometry(args, g, adm0str, adm1str, adm2str)

    return adm0str, adm1str, adm2str


def process_geometry(args, g, adm0str, adm1str, adm2str):

    curISO = g["ISO_CODE"].values[0]

    DTA_A0Path = outPath + "ADM0_" + curISO + ".geojson"
    g.to_file(DTA_A0Path, driver="GeoJSON")

    # Get the ADM1 and ADM2 from geoBoundaries.
    # There should be only RARE exceptions where ADM1 and ADM2 do not exist.
    A0Path = gBPath + curISO + "/ADM0/geoBoundaries-" + curISO + "-ADM0.geojson"
    A1Path = gBPath + curISO + "/ADM1/geoBoundaries-" + curISO + "-ADM1.geojson"
    A2Path = gBPath + curISO + "/ADM2/geoBoundaries-" + curISO + "-ADM2.geojson"
    # If they do not exist use the nearest layer to not have gaps in the world
    if not os.path.isfile(A1Path):
        A1Path = A0Path
    if not os.path.isfile(A2Path):
        A2Path = A1Path

    adm1out = outPath + "ADM1_" + curISO + ".topojson"
    adm2out = outPath + "ADM2_" + curISO + ".topojson"

    if not args.no_clobber or (args.no_clobber and not os.path.isfile(adm1out)):
        adm1cmd = cmd(
            "mapshaper-xl " + A1Path + " -simplify keep-shapes percentage=0.10 "
            " -clip " + DTA_A0Path + " -o format=topojson " + adm1out
        )
        if "Error: JSON parsing error" in adm1cmd.stderr:
            log.error(A1Path)

    if not args.no_clobber or (args.no_clobber and not os.path.isfile(adm2out)):
        adm2cmd = cmd(
            "mapshaper-xl " + A2Path + " -simplify keep-shapes percentage=0.10 "
            " -clip " + DTA_A0Path + " -o format=topojson " + adm2out
        )
        if "Error: JSON parsing error" in adm2cmd.stderr:
            log.error(A2Path)

    adm0str = adm0str + DTA_A0Path + " "
    adm1str = adm1str + outPath + "ADM1_" + curISO + ".topojson "
    adm2str = adm2str + outPath + "ADM2_" + curISO + ".topojson "

    return adm0str, adm1str, adm2str


def join_admins(adm0str, adm1str, adm2str):
    # Join the ADM0 / ADM1 / ADM2s together into one large geom.
    dropFields = "PROV_34_NA,DIST_34_NA,OBJECTID,Shape_Leng,Shape_Area,shapeISO,id,OBJECTID,id,COUNTRY_NA,Shape_Le_1,shapeName,ADM1_NAME,admin1Name,Type,'ISO Code',LEVEL_1,Shape_Length,ISO2,LEVEL2,SHAPE_Leng,SHAPE_Area,DISTRICT,admin2Name,OBJECTID_1"

    A0mapShaperFull = (
        "mapshaper-xl -i "
        + adm0str
        + " "
        + outPath
        + "disputedISO.geojson"
        + " combine-files -merge-layers force"
        + " name=globalADM0"
        +
        # " -simplify weighted " + ratio + "% keep-shapes" +
        " -clean gap-fill-area=10000km2 keep-shapes"
        + " -drop fields="
        + dropFields
        + " -o format=topojson "
        + (outPath + "geoBoundariesCGAZ_ADM0.topojson")
        + " -o format=geojson "
        + (outPath + "geoBoundariesCGAZ_ADM0.geojson")
        + " -o format=shapefile "
        + (outPath + "geoBoundariesCGAZ_ADM0.shp")
    )
    A1mapShaperFull = (
        "mapshaper-xl -i "
        + adm1str
        + " "
        + outPath
        + "disputedISO.geojson"
        + " combine-files -merge-layers force"
        + " name=globalADM1"
        +
        # " -simplify weighted " + ratio + "% keep-shapes" +
        " -clean gap-fill-area=10000km2 keep-shapes"
        + " -drop fields="
        + dropFields
        + " -o format=topojson "
        + (outPath + "geoBoundariesCGAZ_ADM1.topojson")
        + " -o format=geojson "
        + (outPath + "geoBoundariesCGAZ_ADM1.geojson")
        + " -o format=shapefile "
        + (outPath + "geoBoundariesCGAZ_ADM1.shp")
    )
    A2mapShaperFull = (
        "mapshaper-xl -i "
        + adm2str
        + " "
        + outPath
        + "disputedISO.geojson"
        + " combine-files -merge-layers force"
        + " name=globalADM2"
        +
        # " -simplify weighted " + ratio + "% keep-shapes" +
        " -clean gap-fill-area=10000km2 keep-shapes"
        + " -drop fields="
        + dropFields
        + " -o format=topojson "
        + (outPath + "geoBoundariesCGAZ_ADM2.topojson")
        + " -o format=geojson "
        + (outPath + "geoBoundariesCGAZ_ADM2.geojson")
        + " -o format=shapefile "
        + (outPath + "geoBoundariesCGAZ_ADM2.shp")
    )

    cmd(A0mapShaperFull)
    cmd(A1mapShaperFull)
    cmd(A2mapShaperFull)

    cmd(
        "ogr2ogr tmp/CGAZ/geoBoundariesCGAZ_ADM0.gpkg tmp/CGAZ/geoBoundariesCGAZ_ADM0.topojson"
    )
    cmd(
        "ogr2ogr tmp/CGAZ/geoBoundariesCGAZ_ADM1.gpkg tmp/CGAZ/geoBoundariesCGAZ_ADM1.topojson"
    )
    cmd(
        "ogr2ogr tmp/CGAZ/geoBoundariesCGAZ_ADM2.gpkg tmp/CGAZ/geoBoundariesCGAZ_ADM2.topojson"
    )


def dissolve_based_on_ISO_Code(log):
    cmd(
        f"mapshaper-xl ./baseISO.geojson -dissolve fields='ISO_CODE' multipart -o force format=geojson {outPath}baseISO.geojson"
    )
    cmd(
        f"mapshaper-xl ./disputedISO.geojson -dissolve fields='ISO_CODE' multipart -o format=geojson {outPath}disputedISO.geojson"
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument("-n", "--no-clobber", action="store_true")
    args = parser.parse_args()
    log = argparse_log(args)

    preprocess_dta(log)
    dissolve_based_on_ISO_Code(log)
    adm0str, adm1str, adm2str = process_geometries(log, args)
    join_admins(adm0str, adm1str, adm2str)
