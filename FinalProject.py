 # Created By : Ibrahim Aslam
 # Created Date : June 21, 2024
 # Description : Script to perform multiple operations on csv files.


######################## Program imports Starts ##############################
import pandas as pd
import json
import re
import csv
import os
import shutil
import tkinter as tk
############################ Program imports Ends ############################

############################ Functions Starts ################################
def readFromJson():
    with open("config.json", "r") as openfile:
        jsonObj = json.load(openfile)
    return jsonObj

'''def replaceNaN(df,replaceValue):
    df.fillna(replaceValue , inplace=True)
    return  df'''

def replaceNaN(df, replaceValue):
    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            df[column] = df[column].astype(object)
    df.fillna(replaceValue, inplace=True)
    return df

def replaceDelimiter(path,delimiter):
    '''reader = csv.reader(open(path, "r"))
    newPath = "./Output/new.csv"
    writer = csv.writer(open(newPath, "w"), delimiter=delimiter)
    writer.writerows(reader)'''

    df = pd.read_csv(path)
    df.to_csv(path, sep=delimiter, index=False, quoting=csv.QUOTE_ALL)

def useRegEx(path , findRegex , replaceWithRegex):
    with open (path,'r') as file:
       file_content = file.read()

    modified_content = re.sub(findRegex,replaceWithRegex,file_content)

    with open(path,'w') as file:
       file.write(modified_content)



def createAdditionalCols(df , ColHeader, ColValues, separator):

    '''if not all(col in df.columns for col in ColValues):
        raise ValueError("One or more columns specified in ColValues do not exist in the Dataframe.")'''
    for col in ColValues:
        if col not in df.columns:
            df[col] = col
    df[ColHeader] = df[ColValues].apply(lambda row:separator.join(row.values.astype(str)),axis=1)
    return df


def specialOperationsOnCols(df, jsonObj):
    
    for specialOpCol in range(len(jsonObj["processFile"]["specialOperationsOnCols"])):
        print(specialOpCol)
        columnList = jsonObj["processFile"]["specialOperationsOnCols"][specialOpCol]
        #print('This is the list of all the columns' , columnList['columnHeader'])
        firstCheck = bool(columnList.get('datatype'))
        secondCheck = bool(columnList.get('removeRowIfValue'))
        fourthCheck = bool(columnList.get('renameFinalColHeader'))
        thirdCheck = bool(columnList.get('retainRowIfValueContains'))
        print(firstCheck,secondCheck,thirdCheck,fourthCheck)
        if (firstCheck):
            if(columnList['datatype'] == 'str'):
                df[columnList['columnHeader']] = df[columnList['columnHeader']].astype(str)

            elif(columnList['datatype'] == 'int'):
                df[columnList['columnHeader']] = df[columnList['columnHeader']].astype(int)

            elif(columnList['datatype'] == 'bool'):
                df[columnList['columnHeader']] = df[columnList['columnHeader']].astype(bool)

            elif(columnList['datatype'] == 'float'):
                df[columnList['columnHeader']] = df[columnList['columnHeader']].astype(float)

        
        if(secondCheck):
            df = df[df[columnList['columnHeader']] != columnList['removeRowIfValue']]
        
        

        if(columnList['selectNumberOfChar'] == True):
            if(columnList['function'] == 'LEFT' or columnList['function'] == 'left' or columnList['function'] == 'Left'):
                numOfCharExist = bool(columnList.get('numberOfChar'))
                if(numOfCharExist):
                    numOfChars = int(columnList['numberOfChar'])
                    df.loc[:, columnList['columnHeader']] = df[columnList['columnHeader']].str[:numOfChars]
            elif(columnList['function'] == 'RIGHT' or columnList['function'] == 'right' or columnList['function'] == 'Right'):
                numOfCharExist = bool(columnList.get('numberOfChar'))
                if(numOfCharExist):
                    numOfChars = int(columnList['numberOfChar'])
                    df.loc[:, columnList['columnHeader']] = df[columnList['columnHeader']].str[-numOfChars:]
        if(thirdCheck):
            colToCheck = columnList['columnHeader']
            colListToSearch = columnList['retainRowIfValueContains']
            if len(colListToSearch) > 1:
                condition = colListToSearch[0]
                colListToSearch = colListToSearch[1:]
                # Drop rows where either colToCheck or any of colListToSearch is NaN or empty string
                df = df.dropna(subset=[colToCheck] + colListToSearch)
                df = df[(df[colToCheck] != "") & df[colListToSearch].apply(lambda x: x != "").all(axis=1)]
                # Apply AND/OR condition
                df = filtered_df_for_multiple_cols(df, colToCheck, colListToSearch, condition)
            elif len(colListToSearch) == 1:
                colToSearch = colListToSearch[0]
                # Drop rows where either colToCheck or colToSearch is NaN or empty string
                df = df.dropna(subset=[colToCheck, colToSearch])
                df = df[(df[colToCheck] != "") & (df[colToSearch] != "")]
                # Filter rows where colToSearch value is contained within colToCheck value
                df = df[df.apply(lambda row: row[colToSearch] in row[colToCheck], axis=1)]


            '''if (len(columnList['retainRowIfValueContains']) > 2):
                condition = columnList['retainRowIfValueContains'][0]
                for col in range( 1 , len(columnList['retainRowIfValueContains'])):
                    colListToSearch.append(columnList['retainRowIfValueContains'][col])
                df = filtered_df_for_multiple_cols(df, columnList['columnHeader'] , colListToSearch , condition )

            elif len(columnList['retainRowIfValueContains']) == 1:
                colToSearch = columnList['retainRowIfValueContains'][0]
                colToCheck = columnList['columnHeader']
                # Drop rows where either colToCheck or colToSearch is NaN or empty string
                df = df.dropna(subset=[colToCheck, colToSearch])
                df = df[(df[colToCheck] != "") & (df[colToSearch] != "")]
                # Filter rows where colToSearch value is contained within colToCheck value
                df = df[df.apply(lambda row: row[colToSearch] in row[colToCheck], axis=1)]'''
                
        if(fourthCheck):
            df = df.rename(columns={columnList['columnHeader'] : columnList['renameFinalColHeader']})
    return df

def filtered_df_for_multiple_cols(df, col_to_search, cols_to_check, condition_type):
    if condition_type not in ['AND', 'OR']:
        raise ValueError('Condition type must be AND or OR')

    for col in cols_to_check:
        if col not in df.columns:
            raise KeyError(f"Column to check '{col}' not found in DataFrame.")
        
    if col_to_search not in df.columns:
        raise KeyError(f"Column to search '{col_to_search}' not found in DataFrame.")

    if condition_type == 'AND':
        condition = pd.Series([True] * len(df), index=df.index)
    else:
        condition = pd.Series([False] * len(df), index=df.index)

    for check_col in cols_to_check:
        sub_condition = df.apply(lambda row: pd.notna(row[check_col]) and pd.notna(row[col_to_search]) and (row[check_col] in row[col_to_search]), axis=1)
        sub_condition.index = df.index  # Ensure alignment of indices
        
        if condition_type == 'AND':
            condition &= sub_condition
        elif condition_type == 'OR':
            condition |= sub_condition

    df_filtered = df[condition]
    return df_filtered

############################ Functions Ends ##################################

############################ Program Starts ##################################
pd.set_option('max_colwidth', 2000,"display.max_rows", 1000, "display.min_rows", 200, "display.max_columns", None, "display.width", None)

jsonObj = readFromJson()

############################ Process Single File Start ##################################
if (jsonObj['processFileOrMergeTwo'] == True):

    if (jsonObj['useUserDefinedPath'] == True):
        sourceFile = jsonObj['filePath'] + jsonObj["processFile"]["Name"] +"."+ jsonObj["processFile"]["inputFileFormat"]
        print("This is path user:",sourceFile)
    elif(jsonObj['useUserDefinedPath'] == False):
        path=os.getcwd()
        sourceFile = path + "/"+ jsonObj["processFile"]["Name"] +"."+ jsonObj["processFile"]["inputFileFormat"]
        print("This is default user:",sourceFile)

    destinationFolder = './Output/'
    if not os.path.exists(destinationFolder):
        os.makedirs(destinationFolder)
        shutil.copy2(sourceFile, destinationFolder) 
    else:
        shutil.copy2(sourceFile, destinationFolder)
        
    destinationFile = destinationFolder + jsonObj["processFile"]["Name"] +"."+ jsonObj["processFile"]["inputFileFormat"]

######################## Process with Delimiter Start ############################
    if(jsonObj["processFile"]["replaceCommaDelimiter"] == True ):
        delimiter = jsonObj["processFile"]["delimiter"]
        replaceDelimiter(destinationFile , delimiter)
        
        if(jsonObj["processFile"]["useCleanupRegex"] == True):
            useRegEx(destinationFile , jsonObj["processFile"]["cleanupRegexFind"] , jsonObj["processFile"]["cleanupRegexReplaceWith"])

        df = pd.read_csv(destinationFile , delimiter=delimiter)

        df = replaceNaN(df , jsonObj["processFile"]["replaceNaNWith"])

        if(jsonObj["processFile"]["trimColumns"] == True):
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)


        if(len(jsonObj["processFile"]["specialOperationsOnCols"]) > 0):
            df = specialOperationsOnCols(df,jsonObj)

        keyExists = bool(jsonObj["processFile"].get("createAdditionalCols"))
        if (keyExists):
            for createCols in jsonObj['processFile']['createAdditionalCols']:
                createAdditionalCols(df, createCols['ColHeader'] , createCols['ColValues'] , createCols['concatColsSeparator'])

        df.to_csv(destinationFile, sep=delimiter, index=False, quoting=csv.QUOTE_ALL)


######################## Process with Delimiter End ############################
######################## Process without Delimiter Start ############################
    elif(jsonObj["processFile"]["replaceCommaDelimiter"] == False):
        delimiter = ','
        if(jsonObj["processFile"]["useCleanupRegex"] == True):
            useRegEx(destinationFile , jsonObj["processFile"]["cleanupRegexFind"] , jsonObj["processFile"]["cleanupRegexReplaceWith"])

        df = pd.read_csv(destinationFile , delimiter=delimiter)
        df = replaceNaN(df , jsonObj["processFile"]["replaceNaNWith"])

        if(jsonObj["processFile"]["trimColumns"] == True):
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)


        if(len(jsonObj["processFile"]["specialOperationsOnCols"]) > 0):
             df = specialOperationsOnCols(df,jsonObj)

        keyExists = bool(jsonObj["processFile"].get("createAdditionalCols"))
        if (keyExists):
            for createCols in jsonObj['processFile']['createAdditionalCols']:
                createAdditionalCols(df, createCols['ColHeader'] , createCols['ColValues'] , createCols['concatColsSeparator'])

        df.to_csv(destinationFile, sep=delimiter, index=False, quoting=csv.QUOTE_ALL)
        

######################## Process without Delimiter End ############################
############################ Process Single File End ##################################



############################ Merge process 2 Files Start ##################################
elif(jsonObj['processFileOrMergeTwo'] == False):
    print("We have to merge 2 files here now")
   
    df1 = pd.read_csv(jsonObj['mergeOperations']['masterFile'] , delimiter=jsonObj['mergeOperations']['masterFileDelimiter'])
    df2 = pd.read_csv(jsonObj['mergeOperations']['secondaryFile'] , delimiter=jsonObj['mergeOperations']['secondaryFileDelimiter'])

    result = pd.merge(df1 , df2, left_on=[jsonObj['mergeOperations']['masterFileKeyCol']] , right_on=[jsonObj['mergeOperations']['secondaryFileKeyCol']] , how = jsonObj['mergeOperations']['joinType'])
    
    #df.drop(['B', 'C'], axis=1)
    if(len(jsonObj['removeColsAfterMerge']) > 0):
        for i in jsonObj['removeColsAfterMerge']:
            result.drop( [i] , axis=1 , inplace= True)
    
    
    result.to_csv(jsonObj['mergeOperations']['saveFileAs'], sep=jsonObj['mergeOperations']['mergedFileDelimiter'], index=False, quoting=csv.QUOTE_ALL)
    print(result)





print("########################## SUCCESS ###############################")
############################ Merge process 2 Files End ##################################

############################ Program Ends ####################################
