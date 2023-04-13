# Search Hotspots

Search Logs Data Pipeline and Streamlit Dashboard

## Project Description

This project is a data pipeline and Streamlit dashboard that extracts search logs distributions per month and by country. The pipeline is built with Python and utilizes Pandas, Geopandas, and Shapely libraries to manipulate the data and visualize it on a map using a grid-like visualization. The dashboard is built with Streamlit, which allows users to interact with the data and explore it in a user-friendly way.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Data Pipeline](#data-pipeline)
- [Streamlit Dashboard](#streamlit-dashboard)
- [Contributing](#contributing)
- [License](#license)

## Installation

To run this project locally, follow these steps:

1. Clone the repository
2. Install the required dependencies (requirements.txt)
3. Run the Streamlit dashboard

## Run locally
To run the application locally we simply need to execute
```
bash entrypoint.sh
```
within the streamlit-flask folder.

Once the command has been executed we should be able to find the application running on port 5000

Once the Streamlit dashboard is running, users can explore the search logs data by selecting a month and country from the dropdown menus. The data will be displayed on a map, with each grid cell representing a region in the country. Users can hover over the grid cells to see the search logs distribution for that region.

## Data Pipeline

The data pipeline consists of several Python scripts that perform the following tasks:

- Extract data from ADX containing search logs data.
- Clean and preprocess the data.
- Group the data by month and country, and aggregate the search logs counts.
- Write the aggregated data to a new CSV file.

The output of the data pipeline is a set of CSV files that is used by the Streamlit dashboard to visualize the data on a map.

## Streamlit Dashboard

The Streamlit dashboard is a Python script that utilizes the Folium and Streamlit libraries to create a web-based user interface for exploring the search logs data. The dashboard displays the data on a map, with each grid cell representing a region in the country. Users can select a month and country from the dropdown menus to view the search logs distribution for that region.

## Contributing

Contributions to this project are welcome! If you find a bug or would like to suggest an improvement, please open an issue on the project's GitHub page. If you would like to contribute code, please fork the repository and submit a pull request.





