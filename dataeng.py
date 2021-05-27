# -*- coding: utf-8 -*-
import pandas as pd
import os
import sqlite3
pd.set_option('display.max_columns', None)
from io import StringIO, BytesIO, TextIOWrapper
import boto3
import gzip



def processJson(fileDir):
    """Reads Json file to dataframe, cleans file, and returns it

    Parameters
    ----------
    fileDir : str
        The file location of the json file

    Returns
    -------
    DataFrame
        a pandas DataFrame with the clean file
    """
    #Read json to DataFrame
    df = pd.read_json(fileDir)
    #Transpose DataFrame
    df = df.transpose()
    #Turn index into BUSID column and reset index
    df = df.rename_axis("Business - Id").reset_index()
    #Drop business - name columns as it's already in main DataFrame
    df = df.drop(['Business - Name'], axis=1)
    #return clean DataFrame
    return df
    
def processSQLite(fileDirs):
    """Reads sqlite file(s) to dataframe and returns them

    Parameters
    ----------
    fileDir : str
        The file location of the json file

    Returns
    -------
    DataFrame
        a pandas DataFrame with the clean file
    """
    
    try:
        conn = sqlite3.connect(fileDirs[0])
        users = pd.read_sql_query("SELECT * FROM Users2", conn)
        busAttr = pd.read_sql_query("SELECT * FROM business_attributes", conn)
        #Drop duplicate business info on the business attributes table
        busAttr = busAttr.drop_duplicates()
        conn.close()
    except:
        print("sqlite read error")
    
    usersBusAttr = pd.merge(users, busAttr, how = 'left', on = ['Business - Id'])
    return usersBusAttr 
    
def appendDataFrames(dfList):
    """Appends list of DataFrames to combine them

    Parameters
    ----------
    dfList : List
        A list of DataFrames

    Returns
    -------
    DataFrame
        a pandas DataFrame made from appended DataFrames
    """
    #Initialize empty DataFrame
    df = pd.DataFrame()
    #Dynamically append DataFrames to empty DataFrame from dfList
    for dfs in dfList:
        df = df.append(dfs, ignore_index = True)
    #Return final completed DataFrame
    return df

def processCSV(fileDir):
    """Reads csv file to dataframe, cleans file, and returns it

    Parameters
    ----------
    fileDir : str
        The file location of the json file

    Returns
    -------
    DataFrame
        a pandas DataFrame with the clean file
    """
    #Read CSV file to DataFrame
    df = pd.read_csv(fileDir)
    #Remove Columns, Unnamed: 0, Business - Id, and User - ID
    df = df.drop(['Unnamed: 0', 'Business - Id','User - Id'], axis=1)
    #return clean dataframe
    return df

def makeDF():
    #set directory of data files
    directory = "./Data/dataengineering/"
    #initialize lists to hold directories of files by type
    #NOTE: When integrating in WebApp use filenames instead of Dirs
    jsonDirs, csvDirs, sqliteDirs = [], [], []
    #initialize list to hold DataFrames obtrained from processed json files
    jsonDFs, csvDFs = [], []
    #loop through directory to find json, csv and sqlite file directories and store them
    for file in os.listdir(directory):
        if file.endswith('.json'):
            jsonDirs.append(directory + file)
        elif file.endswith('.csv'):
            csvDirs.append(directory + file)
        elif file.endswith('.sqlite'):
            sqliteDirs.append(directory + file)

    #process all json files
    for dirs in jsonDirs:
        jsonDFs.append(processJson(dirs))
    busComp = appendDataFrames(jsonDFs)

    #process all csv files
    for dirs in csvDirs:
        csvDFs.append(processCSV(dirs))
    reviews = appendDataFrames(csvDFs)

    #process SQLite3 tables
    #cleanDf will be the final DataFrame with all the data available
    cleanDf = processSQLite(sqliteDirs)
    cleanDf = pd.merge(cleanDf, reviews, how = 'left', on = ['Review - Id'])
    cleanDf = pd.merge(cleanDf, busComp, how = 'left', on = ['Business - Id'])
    return cleanDf

def computeQueries(df):
    #Find the mean reviews scores by business - Id. 
    meanRevBus = df[['Business - Id', 'Review - Stars']].groupby(['Business - Id'],as_index=False).mean('Review - Stars')
    #Rename columns
    meanRevBus = meanRevBus.rename(columns={'Review - Stars' : 'Average Review Stars'})
    
    #find mean review by top 5 business dense zip codes
    #subset complete df
    dfzip = df[["Business - Id", 'Business - Address', 'Review - Stars']].copy()
    #use str method to get the last 5 chars of address, containing zip codes
    dfzip['Zipcode'] = dfzip['Business - Address'].str[-5:]
    #Create subset of unique business IDs
    dfbus = dfzip.drop_duplicates(['Business - Id'])
    #Calculate top 5 most business dense zip codes by getting the count of unique business IDs by zip code, sorting by descending, and getting top 5 rows
    top5zip = dfbus.groupby(['Zipcode'],as_index=False).size().sort_values(['size'], ascending = False).head(5).Zipcode.tolist()
    #Filter out zip codes that are not in the top 5
    dfzip = dfzip.loc[dfzip.Zipcode.isin(top5zip)]
    #Get the average review score by the top 5 zipcodes
    zipcode = dfzip[['Zipcode', 'Review - Stars']].groupby(['Zipcode'], as_index = False).mean('Review - Stars')
    zipcode = zipcode.rename(columns={'Review - Stars' : "Average Review Stars"})
    
    
    #Calculate Top 10 active reviewers
    user = df.groupby(['User - Id'], as_index = False).size().sort_values(['size'], ascending = False).head(10)
    user = user.rename(columns={'size' : "Number of Reviews Written"})
    return meanRevBus, zipcode,user

def toBucket(df, name):
    bucket = 'jaimevargasdiaztestbucket' # bucket name already created on S3
    csv_buffer = StringIO() # set string file object
    df.to_csv(csv_buffer) # serialize dataframe using StringIO
    s3_resource = boto3.resource('s3',
             aws_access_key_id='AKIA6AL55TG42AXGM2P2',
             aws_secret_access_key='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') # connect to s3 using boto3 library
    
    if name != 'mainDF.gz':
        s3_resource.Object(bucket, name).put(Body=csv_buffer.getvalue()) # save csv
    else:
        buffer = BytesIO()
        with gzip.GzipFile(mode='w', fileobj=buffer) as zipped_file:
            df.to_csv(TextIOWrapper(zipped_file, 'utf8'), index=False)
        s3_object = s3_resource.Object('jaimevargasdiaztestbucket', name)
        s3_object.put(Body=buffer.getvalue()) 
        
df = makeDF()
m,z,u = computeQueries(df)
toBucket(df, 'mainDF.gz')
toBucket(m, 'meanReviewsBus.csv')
toBucket(z, 'mostActiveUsers.csv')
toBucket(u, 'topZipReviews.csv')