# geoBoundaryBot

Confirming geoBoundaries submissions are in the proper format.

## Dev setup

Install Git LFS and mapshaper

```
git lfs install --system --skip-repo
npm install -g mapshaper
```

Clone this repository and `cd` to this folder then run:

```sh
pipenv install # or pip3 install pandas geopandas shapely requests matplotlib rich
git clone --depth=1 https://github.com/wmgeolab/geoBoundaries ../geoBoundaries/
cd ../geoBoundaries/ && git lfs pull
```
