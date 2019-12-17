from datetime import datetime, timedelta

import boto3
import fiona
import geopandas as gpd
import pandas as pd
import requests
from geopandas.tools import sjoin
from shapely.geometry import box

s3 = boto3.resource('s3')


def lambda_handler():
    gdf = download_current_perimeters(n_days=30)
    geojson_string = gdf.to_json()
    obj = s3.Object('tiles.nst.guide', f'wildfire_current/current.geojson')
    obj.put(Body=geojson_string, ContentType='application/geo+json')

# Notes:
# Keep features with non null geometries
# Keep the most recent polygon for each id. Sometimes there are overlapping polygons listed for the same fire from different dates. Note that these overlapping polygons have different GlobalID values, but the same IRWINID

def download_current_perimeters(n_days=30):
    # This URL downloads current perimeters from the NIFC portal
    # See the info page here:
    # https://data-nifc.opendata.arcgis.com/datasets/wildfire-perimeters
    url = 'https://opendata.arcgis.com/datasets/5da472c6d27b4b67970acc7b5044c862_0.zip'
    r = requests.get(url)
    with fiona.BytesCollection(r.content) as fcol:
        crs = fcol.crs
        gdf = gpd.GeoDataFrame.from_features(fcol, crs=crs)

    # Keep rows with non-null geometry
    gdf = gdf[gdf.geometry.notna()]

    # Keep only rows with IRWINID
    # It seems like rows without IRWINID don't have as established data
    gdf = gdf[gdf['IRWINID'].notna()]

    # Keep only rows in the last month
    # First, cast to date
    gdf['DateCurren'] = pd.to_datetime(gdf['DateCurren'])
    # Remove timezone so that I can compare dates easily
    gdf['DateCurren'] = gdf['DateCurren'].dt.tz_localize(None)

    # Find date n_days ago
    today = datetime.now()
    delta = timedelta(days=n_days)
    n_days_ago = today - delta

    # Keep rows since n_days_ago
    gdf = gdf[gdf['DateCurren'] >= n_days_ago]

    # Keep within bounding box
    # Don't want to deal with trail intersection info
    # So just give a general west-coast bbox
    # This is most of CA/OR/WA
    bounds = (-125.59, 32.27, -114.43, 49.14)
    bbox = gpd.GeoDataFrame(geometry=[box(*bounds)], crs={'init': 'epsg:4326'})
    gdf = gdf.to_crs(epsg=4326)
    gdf = sjoin(gdf, bbox, how='inner')

    # Deduplicate rows?
    # Select most recent polygon for a single fire id?

    # Keep only necessary columns
    # Rename cols to standard name
    # Make the name Title Case instead of UPPER CASE

    return gdf
