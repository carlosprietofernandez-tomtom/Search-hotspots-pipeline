import pandas as pd
import json
import geopandas
import os, sys
from tqdm import tqdm
import pycountry
import calendar
import ast
from shapely.geometry import shape, box
import numpy as np
import time
#### FUNCTIONS


def create_grid(lat_range, lon_range, cell_size):
    # Calculate the number of rows and columns in the grid
    rows = int((lat_range[1] - lat_range[0]) / cell_size)
    cols = int((lon_range[1] - lon_range[0]) / cell_size)

    # Calculate the rounded coordinates for each row and column
    lats = np.arange(lat_range[0], lat_range[1], cell_size)
    lats = np.round(lats, 5)
    lons = np.arange(lon_range[0], lon_range[1], cell_size)
    lons = np.round(lons, 5)

    # Create the grid with boundary coordinates for each cell
    grid = [[(lats[y], lats[y+1], lons[x], lons[x+1]) for x in range(cols)] for y in range(rows)]

    return grid


def color_grid(grid, occurrences, color_map):
    # Initialize a dictionary to store the color for each cell
    cell_colors = {}

    # Iterate over the cells in the grid
    for i, row in enumerate(grid):
        for j, cell in enumerate(row):
            # Count the number of occurrences in the cell
            count = 0
            for lat, lon in occurrences:
                if (cell[0] <= lat <= cell[1]) and (cell[2] <= lon <= cell[3]):
                    count += 1

            # Choose the color for the cell based on the number of occurrences
            if count == 0:
                color = "white"
            elif count <= 1:
                color = color_map[0]
            elif count <= 2:
                color = color_map[1]
            elif count <= 3:
                color = color_map[2]
            else:
                color = color_map[3]

            # Store the color for the cell
            cell_colors[(i, j)] = color

    return cell_colors


def getBoundingBox(country, countries_geos):
    country_polygon = countries_geos[countries_geos["ISO_A3"] == country]

    if country_polygon is None:
        raise ValueError(f"Country not found in the geojson: {country}")
    # Get the bounding box coordinates
    min_x, min_y, max_x, max_y = (
        country_polygon.bounds.minx.values[0],
        country_polygon.bounds.miny.values[0],
        country_polygon.bounds.maxx.values[0],
        country_polygon.bounds.maxy.values[0],
    )
    lon_range = [round(min_x, 4), round(max_x, 4)]
    lat_range = [round(min_y, 4), round(max_y, 4)]
    return lat_range, lon_range


def get_grid_sums(coordinates_df, lat_range, lon_range, grid_size):
    coordinates_df["coordinates"] = (
        coordinates_df["location"]
        .apply(lambda x: (json.loads(x)["lat"], json.loads(x)["lon"]))
        .tolist()
    )
    # Create a new column that contain grid cell index
    coordinates_df["grid_index"] = coordinates_df.apply(
        lambda row: (
            int((row["coordinates"][0] - lat_range[0]) / grid_size),
            int((row["coordinates"][1] - lon_range[0]) / grid_size),
        ),
        axis=1,
    )

    # Group by grid cell index and sum counts
    grid_sums = coordinates_df.groupby(["grid_index"])["search_query_counts"].sum()

    return grid_sums


def color_grid_sums(grid, occurrences, color_map):
    # Initialize a dictionary to store the color for each cell
    cell_colors = []

    for index, value in occurrences.items():
        if 0 <= index[0] < len(grid) and 0 <= index[1] < len(grid[0]):
            # Choose the color for the cell based on the number of occurrences
            if value == 0:
                color = "white"
            elif value <= 50:
                color = color_map[0]
            elif value <= 500:
                color = color_map[1]
            elif value <= 5000:
                color = color_map[2]
            else:
                color = color_map[3]

            # Store the color for the cell
            cell_colors.append([color, value, grid[index[0]][index[1]]])

    return cell_colors


def save_data(cell_colors_sums, country, center, month, grid_size):
    # create directory for country if it does not exist
    if not os.path.exists(
        f"apps/searchHotspots/db/{country}/{month.year}_{month.month}"
    ):
        os.makedirs(f"apps/searchHotspots/db/{country}/{month.year}_{month.month}")

    # save monthly df to db
    cell_colors_sums_df = pd.DataFrame(
        cell_colors_sums, columns=["color", "value", "grid_bbox"]
    )
    cell_colors_sums_df = coloring(cell_colors_sums_df)
    cell_colors_sums_df.to_csv(
        f"apps/searchHotspots/db/{country}/{month.year}_{month.month}/{calendar.month_name[int(month.month)]}_{grid_size}.csv",
        index=False,
    )
    cell_colors_sums_df = pd.read_csv(f"apps/searchHotspots/db/{country}/{month.year}_{month.month}/{calendar.month_name[int(month.month)]}_{grid_size}.csv")
    
    # Aggregate monthly counts to total
    aggregate_data(cell_colors_sums_df, country, grid_size)

    # Add country center coordinates file if it does not exist
    if not os.path.exists(f"apps/searchHotspots/db/{country}/center_coordinates.json"):
        data = {"center_coordinates": center}
        with open(
            f"apps/searchHotspots/db/{country}/center_coordinates.json", "w"
        ) as outfile:
            json.dump(data, outfile)


def filter_by_region(searches_df, country_ISO3, center, prev_month, grid_size):

    for region in os.listdir("apps/searchHotspots/data/US_regions"):
        start_time = time.time()
        print(f"processing region {region}")
        country_name = f"{country_ISO3}_{region}"
                    
                    
        region = geopandas.read_file(f"apps/searchHotspots/data/US_regions/{region}")
        region_polygon = shape(region.geometry[0]) 

        filtered_rectangles = []
        searches_aux = []
        searches_df = pd.DataFrame(
            searches_df, columns=["color", "value", "grid_bbox"]
        )  

        searches_df_dict = searches_df.to_dict('records')
        for row in searches_df_dict:
            box_bound = row['grid_bbox']
            rectangleBox = box(box_bound[2], box_bound[0], box_bound[3], box_bound[1])
            if rectangleBox.within(region_polygon):
                filtered_rectangles.append(row)
            else:
                searches_aux.append(row)

        searches_df =  pd.DataFrame(searches_aux)
                
    
        print(f"saving region")
        save_data(
            filtered_rectangles, country_name.split(".geojson")[0], center, prev_month, grid_size
        )
        print(f"time: {start_time-time.time()}")



def coloring(df):

    # compute the log of the values
    df['log_value'] = np.log10(df['value'] + 1)

    # set the number of bins
    num_bins = 6

    # compute the bin edges using log-scale binning
    bin_edges = np.logspace(df['log_value'].min(), df['log_value'].max(), num=num_bins+1)
    bin_edges = np.concatenate((bin_edges[:1], bin_edges[3:]), axis=0)
    # bin the data using the computed bin edges
    df['bin'] = pd.cut(df['value'], bins=bin_edges, labels=False)

    # Define the bin edges
    bins = np.arange(5)
    # Define the bin labels
    labels = [ 'green', 'yellow', 'orange', 'red']

    # Assign labels to each bin
    df['color'] = pd.cut(df['bin'], bins=bins, labels=labels, include_lowest=True, right=False)

    df.drop(['bin','log_value'], inplace=True, axis=1)
    
    return df
    
def aggregate_data(cell_colors_sums_df, country, grid_size):
    total_searches_path = f"apps/searchHotspots/db/{country}/total_{grid_size}.csv"
    total_searches = pd.DataFrame()
    if os.path.exists(total_searches_path):
        total_searches = pd.read_csv(total_searches_path)
    
    concat_df = pd.concat([cell_colors_sums_df, total_searches])

    merged_df = concat_df.groupby(["grid_bbox"])["value"].sum().reset_index()


    merged_df = coloring(merged_df)

    merged_df.to_csv(total_searches_path, index=False)
    

def get_country_names(path):
    files = []
    for file in os.listdir(path):
        country_name = file.split("_")[2].split(".csv")[0]
        files.append(country_name)
    return files


