import pandas as pd
import datetime
import auxiliary_functions.generating_ADX_sample as utils
import urllib.parse

import grid_utils
import geopandas
import pycountry
import os, sys
from tqdm import tqdm
import time
import logging
import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col

spark = (
    SparkSession.builder.master("local[8]")
    .config("spark.driver.memory", "8G")
    .appName("SearchLogs")
    .getOrCreate()
)

pd.set_option("display.max_columns", 100)
version = datetime.datetime.today().strftime("%d-%m-%Y")
logging.basicConfig(filename="monthly_script.log", level=logging.INFO)


def parse_searched_query(x):
    """Mapping function that receives the query string and decodes all encoded characters like %20 -> space

    :param x: String containing encoded query string
    :param x: str
    """
    if not x:
        x = ""
    else:
        x = urllib.parse.unquote(x)
        x = x[: min([idx if y == "." else len(x) for idx, y in enumerate(x)])]
        # x = str(x).replace("\t", "").replace(",", " ").replace("\n", "").replace("+", " ").lower().strip()

    return x


# COMMAND ----------


# COMMAND ----------


def parse_address_and_search_request(df: pd.DataFrame) -> DataFrame:
    """Function that gets the Pandas DF and converts it into a Spark DF, then it parses the searched query strings and
    flattens the "address" Json column which generates new columns for each one of the address components.

    :param df: DataFrame that contains the data as obtained from the sampling process.
    :type df: pd.DataFrame
    :param save_path: Specifies the path on which you want to save the data.
    :type save_path: str
    :return: A Spark DataFrame with all the relevant information
    :rtype: DataFrame
    """
    # Create a spark Dataframe
    df["search_query_counts"] = df["search_query_counts"].astype(str)
    searchesDF = spark.createDataFrame(data=df)

    parsingUDF = udf(lambda x: parse_searched_query(x))
    searchesDF_parsedQuery = searchesDF.withColumn(
        "searched_query", parsingUDF(col("searched_query"))
    )

    return searchesDF_parsedQuery


def prev_month_dates(month):
    today = datetime.datetime.today() - datetime.timedelta(days=month)
    prev_month_last_day = today.replace(day=1) - datetime.timedelta(days=1)

    return prev_month_last_day, str(prev_month_last_day.day)


def grid_process_main(country, countries_geos, centers_df, prev_month):
    grid_sizes = [0.08, 0.022]  # [0.022, 0.1]
    color_map = ["green", "yellow", "orange", "red"]

    print(f"Processing grid for : {country}....")

    coordinates_df = pd.read_csv(
        f"apps/searchHotspots/search-logs/search_logs_{prev_month.month}_{country}.csv"
    )
    coordinates_df.dropna(subset=["location"], inplace=True)
    center = centers_df[centers_df["AFF_ISO"] == country][
        ["latitude", "longitude"]
    ].values.tolist()[0]

    for grid_size in grid_sizes:
        try:
            country_ISO3 = pycountry.countries.get(alpha_2=country.lower()).alpha_3

            lat_range, lon_range = grid_utils.getBoundingBox(
                country_ISO3, countries_geos
            )

            grid = grid_utils.create_grid(lat_range, lon_range, grid_size)

            grid_sums = grid_utils.get_grid_sums(
                coordinates_df, lat_range, lon_range, grid_size
            )

            cell_colors_sums = grid_utils.color_grid_sums(grid, grid_sums, color_map)

            print(f"processing save_data")
            if country_ISO3 == "USA":
                grid_utils.filter_by_region(
                    cell_colors_sums, country_ISO3, center, prev_month, grid_size
                )
            else:
                grid_utils.save_data(
                    cell_colors_sums, country_ISO3, center, prev_month, grid_size
                )
            # grid_utils.save_data(
            #     cell_colors_sums, country_ISO3, center, prev_month, grid_size
            # )

            if os.path.exists(
                f"apps/searchHotspots/search-logs/search_logs_{prev_month.month}_{country}.csv"
            ):
                os.remove(
                    f"apps/searchHotspots/search-logs/search_logs_{prev_month.month}_{country}.csv"
                )
                print("file deleted")
        except Exception as e:
            if os.path.exists(
                f"apps/searchHotspots/search-logs/search_logs_{prev_month.month}_{country}.csv"
            ):
                os.remove(
                    f"apps/searchHotspots/search-logs/search_logs_{prev_month.month}_{country}.csv"
                )
                print("file deleted")

            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print(f"Error calculating grid for {country_ISO3}")
            continue


if __name__ == "__main__":
    today = datetime.datetime.today()
    print(today.day)
    logging.info(f"Starting monthly script at {datetime.datetime.now()}")

    for month in [0]:
        prev_month_last_day, ago = prev_month_dates(month)
        print(prev_month_last_day, ago)
        centers_df = pd.read_csv(
            "apps/searchHotspots/data/countries_centers(clean).csv"
        )

        # country_ISOs = ["AT"]
        country_ISOs = centers_df["ISO"].unique().tolist()
        endpoint_list = (
            None  # (  ## If you are using only 1 endpoint: 'search 2 search'
        )
        #     "search 2 geocode",
        #     "search 2 search",
        # )
        sample = 20_000_000  # Samples and ago should not be higher than 100000 samples/day
        # ago = '90'  # Maximum look back available -> last 3 months = 90 days
        exclude_endpoint = (
            "search 2 nearbySearch",
            "search 2 reverseGeocode",
            "search 2 poiSearch",
        )

        for countries_iso in tqdm(country_ISOs):
            print(f"Getting records for {countries_iso}")
            try:
                responses_dict_requests = utils.address_components_sample_generator(
                    country_list=[countries_iso],
                    end=prev_month_last_day,
                    endpoint_list=endpoint_list,
                    sample=sample,
                    ago=ago,
                    check_query=False,
                    exclude_endpoint_list=exclude_endpoint,
                )
            except Exception as e:
                print(e)
                print(f"Could not get records for {countries_iso}")
                continue

            # for country in [countries_iso]:
            #     responses_dict_requests[country] = parse_address_and_search_request(
            #         responses_dict_requests[country]
            #     )

            try:
                # Saving the sample
                utils.general_dbfs_save_function_dict_of_countries(
                    responses_dict_requests,
                    "results",
                    f"search_logs_{prev_month_last_day.month}",
                )
            except Exception as e:
                print(e)
                print(f"Could not save records for {countries_iso}")
                continue

            ####  Mapping and GeoJson files Load
            countries_geos = geopandas.read_file(
                "apps/searchHotspots/data/countries.geojson",
                driver="GeoJSON",
            )

            try:
                grid_process_main(
                    countries_iso, countries_geos, centers_df, prev_month_last_day
                )

            except Exception as e:
                print(e)
                print(f"Could not get create grid for {countries_iso}")
                continue
