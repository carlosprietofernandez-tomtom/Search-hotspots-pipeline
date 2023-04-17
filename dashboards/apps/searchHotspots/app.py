# Imports
# -----------------------------------------------------------
import streamlit as st
import pandas as pd
import folium
import geopandas
from shapely.geometry import Polygon
import os
import json
import ast
from folium.plugins import Search
from streamlit_folium import folium_static
import io
from zipfile import ZipFile
import tempfile

# -----------------------------------------------------------
# change icon and page name
st.set_page_config(
    page_title="TOMTOM",
    page_icon="https://seeklogo.com/images/T/TomTom-logo-76DD5E06F5-seeklogo.com.jpg",
    layout="wide",
)


# -----------------------------------------------------------
# Data ingestion
# @st.cache
def get_folders_in_directory(directory):
    folders = []
    for file in os.listdir(directory):
        if os.path.isdir(os.path.join(directory, file)) and file != "USA":
            folders.append(file)
    return folders


@st.cache
def get_countries_geos():
    return geopandas.read_file("data/countries.geojson", driver="GeoJSON")


countries = get_folders_in_directory("db")
dates = get_folders_in_directory("db/ESP")
color_map = ["green", "yellow", "orange", "red"]

countries_geos = get_countries_geos()

# -----------------------------------------------------------
st.sidebar.title("Map Options")
# Create a dropdown for the country
selected_country = st.sidebar.selectbox("Select a country", countries)


# -----------------------------------------------------------
@st.cache
def get_country(selected_country):
    with open(f"db/{selected_country}/center_coordinates.json", "r") as infile:
        center_data = json.load(infile)
    center = center_data["center_coordinates"]
    cell_colors_sums = pd.read_csv(f"db/{selected_country}/total_0.08.csv")
    cell_colors_sums["grid_bbox"] = cell_colors_sums["grid_bbox"].apply(
        lambda x: ast.literal_eval(x)
    )

    return cell_colors_sums, center


# -----------------------------------------------------------

# Main
# -----------------------------------------------------------
# Create a title for your app
st.title("Search Hotspots Dashboard")
# st.info("Hello! In this Dashboard you can quickly check the hottest spots our users have searched for the last 90 days! Keep in mind the data gets updated weekly 😀")
# import bootstrap
st.sidebar.markdown(
    '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.0.0/dist/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">',
    unsafe_allow_html=True,
)
st.sidebar.markdown(
    """ 
    <div class="alert alert-info" role="alert"> 
       <!-- <h4 class="alert-heading">ℹ️&nbsp; Relative opportunity value based on operational revenue</h4> -->
       Hello! In this Dashboard you can quickly check the hottest spots our users have searched for <b> since December 1st!</b> 
       <br>Keep in mind the data gets updated monthly 😀
        <hr> 
        <ul class="d-legend">
            <li>&nbsp;&nbsp;<div style="float: left; width: 20px; height: 20px; margin: 0px; background: #00FF00;">
                </div><b>: P4 </b> 
            </li>
            <li>&nbsp;&nbsp;<div style="float: left; width: 20px; height: 20px; margin: 0px; background: #FFFF00;">
                </div><b>: P3 </b> 
            </li>
            <li>&nbsp;&nbsp;<div style="float: left; width: 20px; height: 20px; margin: 0px; background: #FFA500;">
                </div><b>: P2 </b> 
            </li>
            <li>&nbsp;&nbsp;<div style="float: left; width: 20px; height: 20px; margin: 0px; background: #FF0000;">
                </div><b>: P1 </b> 
            </li> 
        </ul> 
    </div> 
    """,
    unsafe_allow_html=True,
)
col1, col2, col3, = st.columns(3)
with col1:
    hide_map = st.checkbox('Hide Visualization')
# Read the JSON file # -----------------------------------------------------------
try:
    cell_colors_sums, center = get_country(selected_country)

    priority_dict = {"green": "P4", "yellow": "P3", "orange": "P2", "red": "P1"}

    ### MAP CREATION # -----------------------------------------------------------

    m = folium.Map(location=center, zoom_start=5, prefer_canvas=True)

    # Create a layer with the colored rectangles

    # Iterate over the cells in the grid and draw a rectangle for each cell
    for color_level in color_map:
        colored_rectangles = folium.FeatureGroup(
            name=f"{priority_dict[color_level]}", min_zoom=6, max_zoom=8
        )
        specific_colored_df = cell_colors_sums[cell_colors_sums["color"] == color_level]

        cell_colors_sums_dict = specific_colored_df.to_dict("records")
        for row in cell_colors_sums_dict:
            box = row["grid_bbox"]
            rect = folium.Rectangle(
                bounds=[(box[0], box[2]), (box[1], box[3])],
                color=color_level,
                weight=0.5,
                fill=True,
                # tooltip=folium.Tooltip(f"Count:{row['value']} <br> Bounds:{box}"),
            ).add_to(colored_rectangles)

        colored_rectangles.add_to(m)
    print("Map generated!!!")
    # Add a layer control widget to the map
    if not hide_map:
        folium.LayerControl().add_to(m)
        folium_static(m, width=2048, height=700)

    st.markdown(
        """
        <style>
        iframe {
            width: 100%;
        }
        .css-18e3th9{
            padding-top:1rem;
        }
        .css-1vq4p4l {
            padding: 1.5rem 1rem 1.5rem;
        }
        .d-legend{
            list-style: none;
        }
        .d-legend li{
            list-style: none;
            margin-left:0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    grid_dict = {"Morton Tile 10": 0.08, "Morton Tile 14": 0.022}
    col1, col2, col3 = st.columns(3)
    with col1:
        selected_grid_size = st.selectbox(
            "Select precision for download", ["Morton Tile 10", "Morton Tile 14"]
        )

    @st.cache
    def convert_df(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        polygons = []

        for bbox in df["grid_bbox"]:
            coords = [
                (float(bbox[2]), float(bbox[0])),
                (float(bbox[2]), float(bbox[1])),
                (float(bbox[3]), float(bbox[1])),
                (float(bbox[3]), float(bbox[0])),
            ]
            polygon = Polygon(coords)
            polygons.append(polygon)
        gdf = geopandas.GeoDataFrame(
            df[["value", "color"]], geometry=polygons, crs="epsg:4326"
        )

        return gdf

    @st.cache
    def save_shapefile_with_bytesio(dataframe, directory):
        dataframe.to_file(f"{directory}/user_shapefiles.shp", driver="ESRI Shapefile")
        zipObj = ZipFile(f"{directory}/user_shapefiles_zip.zip", "w")
        zipObj.write(f"{directory}/user_shapefiles.shp", arcname="user_shapefiles.shp")
        zipObj.write(f"{directory}/user_shapefiles.cpg", arcname="user_shapefiles.cpg")
        zipObj.write(f"{directory}/user_shapefiles.dbf", arcname="user_shapefiles.dbf")
        zipObj.write(f"{directory}/user_shapefiles.prj", arcname="user_shapefiles.prj")
        zipObj.write(f"{directory}/user_shapefiles.shx", arcname="user_shapefiles.shx")
        zipObj.close()

    def export(selected_country, sel_grid_size):
        if "USA" in selected_country:
            export_df = pd.read_csv(f"db/USA/total_{sel_grid_size}.csv")
        else:
            export_df = pd.read_csv(f"db/{selected_country}/total_{sel_grid_size}.csv")
        export_df["grid_bbox"] = export_df["grid_bbox"].apply(
            lambda x: ast.literal_eval(x)
        )
        gdf = convert_df(export_df)
        with tempfile.TemporaryDirectory() as tmp:
            save_shapefile_with_bytesio(gdf, tmp)
            return open(f"{tmp}/user_shapefiles_zip.zip", "rb")

    if not cell_colors_sums.empty:
        if "USA" in selected_country:
            st.download_button(
                label="Download data as shapefile (.shp)",
                data=export(selected_country, grid_dict[selected_grid_size]),
                file_name=f"Full_USA_{grid_dict[selected_grid_size]}.zip",
                mime="application/zip",
            )
        else:
            st.download_button(
                label="Download data as shapefile (.shp)",
                data=export(selected_country, grid_dict[selected_grid_size]),
                file_name=f"{selected_country}_{grid_dict[selected_grid_size]}.zip",
                mime="application/zip",
            )

except Exception as e:
    print(e)
    st.header(f"Could not load data for: {selected_country}")
