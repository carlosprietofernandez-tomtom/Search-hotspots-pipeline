# Databricks notebook source
import pandas as pd
import numpy as np
# from maps_analytics_utils.connections import adx, connections_utils
from auxiliary_functions.adx_utils import get_adx_secrets, execute_adx_query
import typing
import json
from datetime import date, datetime, timedelta
import math
import os
import urllib.parse
import glob
import shutil

from threading import current_thread

import multiprocessing.pool

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Building the functions for ADX:

# COMMAND ----------

def query_addresses_new_OnlineSearch(
    country_code: str or None,
    endpoint_list: typing.List[str] or typing.Tuple[str] or None = None,
    exclude_endpoint_list: typing.List[str] or typing.Tuple[str] or None = ('search 2 poiSearch'),
    sample: int or None = 100000,
    ago: int or None = 365,
    start: datetime or None = (date.today() - timedelta(days=15)),
    end: datetime or None = (date.today() - timedelta(days=1)),
    check_query: bool = False,
) -> str:

    """Function that generates the string query in KQL (kusto query language) to perform in ADX.

    :param country_names: List of names the country can have. Most countries have a lot of denominations and in ADX, the countries denominations are not consistent. So, in order to obtain all possible results, we pass this argument that contain a lot of possible country denominations. This are generated using the "get_country_spellings" function.
    :type country_names: typing.List[str] or typing.Tuple[str] or None
    :param endpoint_list: List of the endpoints you want to filter out for your query, defaults to None, which means no endpoint will be filtered out. 
    :type endpoint_list: typing.List[str] or typing.Tuple[str] or None, optional
    :param exclude_endpoint_list: List of endpoints to exclude from the query, in case you don't want them to appear. Defaults to "search 2 poiSearch" since the poi endpoint doesn't provide relevant information for the india process.
    :type exclude_endpoint_list: typing.List[str] or typing.Tuple[str] or str or None
    :param developer_emails_list: List that determines which emails the query should remove from TT developers (like maps analytics). You can pass elements within a list and they will be removed from the search, defaults to ('maps.analytics.metrics@groups.tomtom.com', ''), which means only the maps analytics emails will be exluded.
    :type developer_emails_list: typing.List[str] or typing.Tuple[str] or None, optional
    :param sample: Sample size you want to extract, defaults to 100000
    :type sample: int, optional
    :param ago: Time (in days) you want the query to include, defaults to 365, which means all queries within 365 days back will be included in the response.
    :type ago: int or None, optional
    :param check_query: Boolean that allows to print the query that was passed to kusto in order to debug. Only switch to True if you are having problems with the query response or the number of responses. Defaults to False, which means that the query shouldn't be printed.
    :type check_query: bool, optional.
    :return: The string to pass to the ADX instance in order to get the response for a specific country.
    :rtype: str
    """

    if ago is not None:
        look_back = f"""
                    let timeStart = datetime({start.strftime("%Y-%m-%d")});
                    let timeEnd = datetime({end.strftime("%Y-%m-%d")});
                    """
    else:
        look_back = ""

    if exclude_endpoint_list is not None:
        exclude_endpoint_line = f"| where method_name !in~ {str(exclude_endpoint_list)}"
    else:
        exclude_endpoint_line = ""

    if endpoint_list is not None:
        endpoint_string = f"| where method_name in~ {str(endpoint_list)}"
    else:
        endpoint_string = ""

    if sample is not None:
        sample_string = f"| sample {sample}"
    else:
        sample_string = f"| sample 1000"


    building_string = f'''
                        {look_back}
                        let SearchNormalRequests = (
                            database("ttapianalytics-westeu-db-source").
                            OnlineSearchData
                            | where client_received_end_timestamp between(timeStart .. timeEnd)
                            | where not(is_error ) and (response_status_code == 200) and (developer_email != "lnspuplacesowners@groups.tomtom.com") and not (developer_email contains "@tomtom.com") and (sessionId == '') and (searched_query != "2655%20mannheim%20rd.json")
                            {endpoint_string}{exclude_endpoint_line}
                            | project ['Tracking-ID']=request_header_Tracking_Id
                        );
                        let SearchTypedRequests = (
                            database("ttapianalytics-westeu-db-source").
                            OnlineSearchData
                            | where client_received_end_timestamp between(timeStart .. timeEnd)
                            | where not(is_error) and (response_status_code ==200) and (developer_email != "lnspuplacesowners@groups.tomtom.com") and not (developer_email contains "@tomtom.com") and (sessionId != '') and (searched_query != "2655%20mannheim%20rd.json")
                            {endpoint_string}{exclude_endpoint_line}
                            | summarize hint.strategy=shuffle arg_max(client_received_end_timestamp , *) by sessionId
                            | project ['Tracking-ID']=request_header_Tracking_Id
                        );
                        let SearchRequests = (
                            union SearchNormalRequests, SearchTypedRequests
                        );
                        let TopResults = (
                            database("ttapianalytics-onlineSearch").
                            OnlineSearchResults
                            | where timestamp between(timeStart .. timeEnd)
                            | where (rank == 0) and (['Tracking-ID'] != 'DMS') and (countryCode == '{country_code}')
                            | where matchConfidence.score >= 0.80 or isnull(matchConfidence.score)
                            | project-away timestamp, rank
                            | distinct ['Tracking-ID'], location = tostring(location)
                            {sample_string}
                        );
                        let SearchResults = (
                            TopResults
                            | join hint.strategy=shuffle kind=inner SearchRequests on ['Tracking-ID']
                            | project-away ['Tracking-ID1']
                        );
                        SearchResults
                        | summarize search_query_counts = count() by tostring(location)
                        | sample 100000
                    '''
    if check_query:
        print('THIS IS THE QUERY YOU EXECUTED ON ADX:')
        print(building_string)
        print('\n')
    
    return building_string

def get_country_logs_multiThreading(
    country: str, end: datetime, endpoint_list: list or tuple or None = None, sample: int=10000, 
    ago: int = 365, exclude_endpoint_list: list or tuple or str or None = ('search 2 poiSearch'),
    check_query: bool = False
) -> pd.DataFrame:
    """Gets search logs for a given country
    :param country: string of ISO-2 code of a country ('ES', not 'ESP').
    :type country: str
    :param endpoint_list: List of the endpoints you want to filter out for your query, defaults to None, which means no endpoint will be filtered out. 
    :type endpoint_list: typing.List[str] or typing.Tuple[str] or None, optional
    :param sample: Initial sample size, defaults to 10000
    :type sample: int, optional
    :param endpoints: Tuple that contains the endpoints you want to search for. This parameter should only be filled if the value of "specify_search_method" is True. Possible values are: search 2 search, search 2 structuredGeocode, search 2 nearbySearch, search 2 geocode, search 2 poiSearch, search 2 categorySearch, etc...
    :type endpoints: tuple(str)
    :param developer_emails_list: List that determines which emails the query should remove from TT developers (like maps analytics). You can pass elements within a list and they will be removed from the search, defaults to ('maps.analytics.metrics@groups.tomtom.com', ''), which means only the maps analytics emails will be exluded. Defaults to None.
    :type developer_emails_list: typing.List[str] or typing.Tuple[str] or None, optional
    :param ago: Include how much time (in days) you want to include for the logs that will be returned. Defaults to 365, which means that logs from up to a year back from the date this is run will be included.
    :type ago: int or None, optional
    :param exclude_endpoint_list: List of endpoints to exclude from the query, in case you don't want them to appear. Defaults to "search 2 poiSearch" since the poi endpoint doesn't provide relevant information for the india process.
    :type exclude_endpoint_list: typing.List[str] or typing.Tuple[str] or str or None
    :param check_query: Boolean that allows to print the query that was passed to kusto in order to debug. Only switch to True if you are having problems with the query response or the number of responses. Defaults to False, which means that the query shouldn't be printed.
    :type check_query: bool, optional.
    :return: DataFrame with logs for the specified search parameters.
    :rtype: pd.DataFrame
    """

    end = end
    start = end - timedelta(days=1)
    addresses_df = pd.DataFrame()
    num_threads = 15
    iterations = 0
    while iterations < int(ago):
        
        if (int(ago) - (iterations)) < num_threads:
            num_threads = (int(ago) - (iterations))
        print(f"ITERATION {int(iterations/num_threads)}: days {iterations } to {(iterations + num_threads)}")
        
        countries_queries = []
        for i in range(num_threads):
            countries_queries.append(query_addresses_new_OnlineSearch(
                country_code=country,
                endpoint_list=endpoint_list,
                sample=int(sample/int(ago)),
                exclude_endpoint_list=exclude_endpoint_list,
                ago=ago,
                start=start,
                end=end,
                check_query=check_query,
            ))
            # print(f"Getting records for {start} - {end}...")
            start = start - timedelta(days=1)
            end = end - timedelta(days=1)

        # a thread pool that implements the process pool API.
        pool = multiprocessing.pool.ThreadPool(processes=num_threads)
        return_list = pool.map(makeRequest, countries_queries, chunksize=1)
        pool.close()

        for status, data in return_list:
            addresses_df = pd.concat([addresses_df, data])

        # addresses_df = deduplicate_on(addresses_df, 'searched_query', 'search_query_counts')    
        print(f"TOTAL: {addresses_df.shape[0]} Records ")
        iterations += num_threads
    
    addresses_df = deduplicate_on(addresses_df, 'location', 'search_query_counts')
    print(f"FINAL RESULT: {addresses_df.shape[0]} Records ")
    # parse_address_and_search_request(addresses_df)
    return addresses_df
# COMMAND ----------
def makeRequest(query_country):
    """ Function that receives an ADX query string and assigns it to each of the active threads that performs an ADX call.
    
    :param query_country: ADX Query for a specific day and specific country
    :param query_country: str
    :return: Returns the response from ADX.
    :rtype: list
    """  
    try:
        thread = current_thread()
        # print(f'Worker thread: name={thread.name}, idnet={get_ident()}, id={get_native_id()}')

        tenant_id, client_id, secret_value, secret_id = get_adx_secrets()
        partial_addresses_df, _ = execute_adx_query(
            query=query_country,
            cluster="https://ttapianalyticsadxpweu.westeurope.kusto.windows.net",
            database="ttapianalytics-onlineSearch",
            client_id=client_id,
            secret_id=secret_id,
            tenant_id=tenant_id,
        )
        return "success", partial_addresses_df
    except:
        return "error", "Error"

# COMMAND ----------

def parse_searched_query(x) :
    """ Mapping function that receives the query string and translates all coded charactes like %20 -> space
    
    :param x: Query value for each row containing encoded query string
    :param x: str
    :return: Returns the decoded query string.
    :rtype: str
    """  
    if not x:
        x = ''
    else:
        x = urllib.parse.unquote(x)
        x = x[:min([idx if y == '.' else len(x) for idx, y in enumerate(x) ])]
    
    return x

# COMMAND ----------


# COMMAND ----------

def parse_address_and_search_request(df: pd.DataFrame) -> pd.DataFrame:
    """Function that gets the DataFrame and parses the address and generates columns for each of its components and also parses the search_request.
    
    :param df: DataFrame that contains the data as obtained from the sampling process from ADX.
    :type df: pd.DataFrame
    :param save_path: Specifies the path on which you want to save the data.
    :type save_path: str
    :return: A DataFrame with the parsed relevant responses
    :rtype: pd.DataFrame
    """
    df['searched_query'] = df['searched_query'].map(parse_searched_query)

    # We convert the address string to a dictionary so we can apply the then use the utility pandas has to transform dictionaries into columns
    df['address'] = df['address'].apply(lambda x :json.loads(x))

    df = pd.concat([df.drop(['address'], axis=1), df['address'].apply(pd.Series)], axis=1)

    return df

# COMMAND ----------
def deduplicate_on(df: pd.DataFrame, on_column: str, counts_column: str) -> pd.DataFrame:
    """ Function that deduplicates search logs

    :param df: Dataframe to perform the deduplication on 
    :type df: pd.DataFrame
    :param on_column: Name of row to perform the deduplication on
    :type on_column: str
    :param counts_column: Name of the row containing the counts values
    :type counts_column: str
    :return: Returns the same dataframe after performing deduplication.
    :rtype: pd.DataFrame
    """
    df_counts = df.groupby([on_column])[counts_column].sum().reset_index()
    df_to_compare = df.drop_duplicates(subset=[on_column], keep='first').drop(counts_column, axis=1)
    return df_to_compare.merge(df_counts, how='right', on=on_column)
# MAGIC %md
# MAGIC ### Building a function for sample generation for multiple countries

# COMMAND ----------

def address_components_sample_generator(
    country_list: list, end:datetime, endpoint_list: list or tuple or None = None, sample: int = 10000, 
    exclude_endpoint_list: list or tuple or None = ('search 2 poiSearch'),
    ago: int = 31, check_query: bool = False
) -> dict:
    '''
    Function that receives the list of countries you want to get the sample for in ISO-2 code, a list of endpoints you want to include in your search, etc. It returns a dictionary with the responses for each of the countries in the list.
    
    :param country_list: List of countries to use in ISO-2 code! For example, if you want the results for Spain and the United States, you should pass country_list = ['ES', 'US'].
    :type country_list: list
    :param endpoint_list: List of the endpoints you want to filter out for your query, defaults to None, which means no endpoint will be filtered out. 
    :type endpoint_list: typing.List[str] or typing.Tuple[str] or None, optional
    :param sample: Specify the sample size you want for each query, defaults to 10000
    :type sample: int, optional
    :param developer_emails_list: List that determines which emails the query should remove from TT developers (like maps analytics). You can pass elements within a list and they will be removed from the search, defaults to ('maps.analytics.metrics@groups.tomtom.com', ''), which means only the maps analytics emails will be exluded. Defaults to None.
    :type developer_emails_list: typing.List[str] or typing.Tuple[str] or None, optional
    :param exclude_endpoint_list: List of endpoints to exclude from the query, in case you don't want them to appear. Defaults to "search 2 poiSearch" since the poi endpoint doesn't provide relevant information for the india process.
    :type exclude_endpoint_list: typing.List[str] or typing.Tuple[str] or str or None
    :param ago: Time (in number of days) that should be included starting from today's date and going back "ago" days, defaults to 365, which means the results will only include queries from up to 365 days back.
    :type ago: int, optional
    :param check_query: Boolean that allows to print the query that was passed to kusto in order to debug. Only switch to True if you are having problems with the query response or the number of responses. Defaults to False, which means that the query shouldn't be printed.
    :type check_query: bool, optional.
    :return: Returns a dictionary with the countries as keys and the query response dataframes as values for each country.
    :rtype: dict
    '''
    # Create an empty dictionary where we will store all the samples by country name:
    country_dict = {}
    
    # Iterate through the country list and call the get_country_logs function on each country:
    for country in country_list:
        sample_df = get_country_logs_multiThreading(
            country=country,  end=end, endpoint_list=endpoint_list, sample=sample,
            ago=ago, exclude_endpoint_list=exclude_endpoint_list, check_query=check_query
        )
        
        country_dict[country] = sample_df
        
    return country_dict


#### SAVING THE SAMPLE ####
def general_dbfs_save_function_dict_of_countries(
    dictionary: dict, path: str, name_append: str) -> None:
    """Function that saves the results of the dataframe you pass into different files following the structure: '{path}/{name_append}_{country}'.
    
    :param dictionary: Dictionary that contains the countries used to generate the samples as keys and the DataFrames of the samples as values.
    :type dictionary: dict
    :param path: String of the base path on which you want to save the content. This will be the base on which you build the saving path.
    :type path: str
    :param name_append: The extension you want to paste after you base path. This will be the name of your file in the folder of the base path, without the extension!!
    :type name_append: str
    """
    for country in dictionary.keys():
        df = dictionary[country]
        file_path = f'apps/searchHotspots/search-logs/{name_append}_{country}'

        # df.write\
        #     .mode("overwrite")\
        #     .parquet((file_path + '_parquet'))

        # df.write\
        #     .mode("overwrite")\
        #     .option("escape", "\"")\
        #     .option("header",True)\
        #     .option("delimiter",";")\
        #     .csv((file_path))
        
        # csv_files = glob.glob(os.path.join(file_path , "*.csv"))
        # query_counts_df = pd.concat((pd.read_csv(f, sep=';') for f in csv_files), ignore_index=True)
        df.to_csv(f'{file_path}.csv', index=False)
        # shutil.rmtree(file_path, ignore_errors=False, onerror=None)
        
        print(f'{country.upper()} is done in {file_path}!!')