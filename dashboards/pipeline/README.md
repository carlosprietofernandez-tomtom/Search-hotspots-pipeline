EXECUTING THE COORDINATE EXTRACTION PROCESS:

Introduction:
The idea of this process is to build an understanding of how are the search logs distributed geographically. This is divided into two parts:
- Sample Generation:  retrieves and preprocesses data from the search logs stored in the ADX database and outputs them to a csv file format.
- Admin Area Aggregation: The sample generated is processed to generate a csv with the data aggregated by the different Admin Area Types.

Folder structure:
The folder has a few folders and executable notebook that we should consider. But first, lets look at the general structure of the folder:

main_folder
│   README.md
│   1.0-ADX_sample-generation.ipynb          --> RUN FIRST
|   2.0_admin_area_aggs_spark.ipynb   		 --> RUN SECOND
│
└───auxiliary_functions
│      generating_ADX_sample.py --> Makes the calls to ADX, where search logs are stored
|      useful_functions.py      --> Extra functions used for general purposes
│   
└───data  --> Where the parquet data obtained from ADX should be stored
│   
└───results   --> Where the final CSVs will be stored
│   
└───wheels   --> Library used in the Search (Analytics) Team to provide access to ADX


PROCESS:
The process you should follow is this:

1. Check your dependencies are installed. If you are not sure, simply go to the needed_dependencies.txt file in the main_folder and pip install all of them on your environment (Anaconda is recommended for this). 
NOTE: You can also go to step (3) and run the first cell in the notebook, then restart the Kernel (execution process of the notebook). This will also install most of the needed libraries, but you will still need to install Python, pandas and geopandas in the environment.

2. Access your preferred Jupyter Notebook style IDE. It could be VS Code, Jupyter Notebooks, Jupyterlab or any other of your choice.

3. Access the notebook called 1.0-ADX_sample-generation.ipynb in the main_folder, from your notebook handler. 

4. Go to the "Setting up country to use" cell and set your parameters. You should pass the country or countries ISO2 codes. For example if you want to get the results for Portugal, Uruguay, you should pass: ['PT', 'UY'].

5. Once all parameters are set, simply execute all the notebook. At the end of the execution, you should have as many "_parquet" directories in the data folder as countries you passed in the list. So in our example, you should have 2 directories corresponding to the search logs in PT and UY . The files will have a similar name to: search_logs_"today's date"_"country". You should also have the resulting CSVs in the results folder called search_logs_"today's date"_"country".csv

6. Now access the notebook called "2.0_admin_area_aggs_spark.ipynb" in the main_folder, from your notebook handler.

7. Go to the "Setting the parameters for the entire process" section and set up the parameters you want to use. This process builds from the data gathered from the step (6), so you should set the specific country's parquet directory from the data folder to be the search_logs_coordinates variable. Then execute the entire notebook (FROM TOP TO BOTTOM!!). 

8. By the end of step (9) you will have the second ".csv" file on the "/results" folder that contains the file with the data aggregated by admin area

IMPORTANT NOTE: There are two lines commented on the admin_areas_aggregation_process function that will remove outliers from your data. Please look into those in case this is interesting for you.