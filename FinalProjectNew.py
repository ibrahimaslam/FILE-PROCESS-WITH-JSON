######################## Program imports Starts ##############################
import pandas as pd
import json
import re
import csv
import os
import shutil
############################ Program imports Ends ############################

############################ Functions Starts ################################
def readFromJson():
    with open("config.json", "r") as openfile:
        jsonObj = json.load(openfile)
    return jsonObj

def replaceNaN(df, replaceValue):

    df.fillna(replaceValue, inplace=True)
    return df

def replaceDelimiter(path, delimiter):

    df = pd.read_csv(path)
    df.to_csv(path, sep=delimiter, index=False, quoting=csv.QUOTE_ALL)

def useRegEx(path, findRegex, replaceWithRegex):
    with open(path, 'r') as file:
        file_content = file.read()
    modified_content = re.sub(findRegex, replaceWithRegex, file_content)
    with open(path, 'w') as file:
        file.write(modified_content)

def createAdditionalCols(df, ColHeader, ColValues, separator):
    
    for col in ColValues:
        if col not in df.columns:
            df[col] = col
    df[ColHeader] = df[ColValues].apply(lambda row: separator.join(row.values.astype(str)), axis=1)
    return df

def specialOperationsOnCols(df, jsonObj):
    
    for specialOpCol in jsonObj["processFile"]["specialOperationsOnCols"]:
        colHeader = specialOpCol.get("columnHeader")
        datatype = specialOpCol.get("datatype")
        removeRowIfValue = specialOpCol.get("removeRowIfValue")
        renameCol = specialOpCol.get("renameFinalColHeader")
        retainRowIfValueContains = specialOpCol.get("retainRowIfValueContains")
        selectNumberOfChar = specialOpCol.get("selectNumberOfChar")
        function = specialOpCol.get("function", "").upper()
        numberOfChar = specialOpCol.get("numberOfChar", 0)

        if datatype:
            if datatype == "str":
                df[colHeader] = df[colHeader].astype(str)
            elif datatype == "int":
                df[colHeader] = df[colHeader].astype(int)
            elif datatype == "bool":
                df[colHeader] = df[colHeader].astype(bool)
            elif datatype == "float":
                df[colHeader] = df[colHeader].astype(float)

        if removeRowIfValue:
            df = df[df[colHeader] != removeRowIfValue]

        if retainRowIfValueContains:
            colListToSearch = retainRowIfValueContains[1:]
            condition = retainRowIfValueContains[0]
            df = filtered_df_for_multiple_cols(df, colHeader, colListToSearch, condition)

        if selectNumberOfChar and numberOfChar > 0:
            if function == "LEFT":
                df[colHeader] = df[colHeader].str[:numberOfChar]
            elif function == "RIGHT":
                df[colHeader] = df[colHeader].str[-numberOfChar:]

        if renameCol:
            df.rename(columns={colHeader: renameCol}, inplace=True)

    return df

def filtered_df_for_multiple_cols(df, col_to_search, cols_to_check, condition_type):
    if condition_type not in ['AND', 'OR']:
        raise ValueError('Condition type must be "AND" or "OR"')

    for col in cols_to_check:
        if col not in df.columns:
            raise KeyError(f"Column '{col}' not found in DataFrame.")
    
    if col_to_search not in df.columns:
        raise KeyError(f"Column to search '{col_to_search}' not found in DataFrame.")
    if condition_type == 'AND':
        condition = pd.Series([True] * len(df), index=df.index)
    else:
        condition = pd.Series([False] * len(df), index=df.index)

    for check_col in cols_to_check:
        sub_condition = df.apply(lambda row: pd.notna(row[col_to_search]) 
                                             and pd.notna(row[check_col]) 
                                             and row[check_col].strip().lower() not in row[col_to_search].strip().lower(), axis=1)
        sub_condition.index = df.index  

        if condition_type == 'AND':
            condition &= sub_condition
        elif condition_type == 'OR':
            condition |= sub_condition

    df_filtered = df[condition]
    return df_filtered

############################ Functions Ends ##################################

############################ Program Starts ##################################
pd.set_option('max_colwidth', 2000, "display.max_rows", 1000, "display.max_columns", None)

jsonObj = readFromJson()

if jsonObj['processFileOrMergeTwo']:
    if jsonObj['useUserDefinedPath']:
        sourceFile = jsonObj['filePath'] + jsonObj["processFile"]["Name"] + "." + jsonObj["processFile"]["inputFileFormat"]
    else:
        sourceFile = os.path.join(os.getcwd(), jsonObj["processFile"]["Name"] + "." + jsonObj["processFile"]["inputFileFormat"])

    destinationFolder = './Output/'
    os.makedirs(destinationFolder, exist_ok=True)
    destinationFile = os.path.join(destinationFolder, jsonObj["processFile"]["Name"] + "." + jsonObj["processFile"]["inputFileFormat"])
    shutil.copy2(sourceFile, destinationFile)

    delimiter = jsonObj["processFile"].get("delimiter", ',')
    if jsonObj["processFile"]["replaceCommaDelimiter"]:
        replaceDelimiter(destinationFile, delimiter)

    if jsonObj["processFile"]["useCleanupRegex"]:
        useRegEx(destinationFile, jsonObj["processFile"]["cleanupRegexFind"], jsonObj["processFile"]["cleanupRegexReplaceWith"])

    df = pd.read_csv(destinationFile, delimiter=delimiter)
    df = replaceNaN(df, jsonObj["processFile"]["replaceNaNWith"])

    if jsonObj["processFile"]["trimColumns"]:
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip()

    if jsonObj["processFile"]["specialOperationsOnCols"]:
        df = specialOperationsOnCols(df, jsonObj)

    if jsonObj["processFile"].get("dropDupesBasedonKey"):
        df = df[df.duplicated(subset=['ID'], keep=False)]

    df.to_csv(destinationFile, sep=delimiter, index=False, quoting=csv.QUOTE_ALL)

else:
    df1 = pd.read_csv(jsonObj['mergeOperations']['masterFile'], delimiter=jsonObj['mergeOperations']['masterFileDelimiter'])
    df2 = pd.read_csv(jsonObj['mergeOperations']['secondaryFile'], delimiter=jsonObj['mergeOperations']['secondaryFileDelimiter'])

    result = pd.merge(df1, df2, left_on=jsonObj['mergeOperations']['masterFileKeyCol'], right_on=jsonObj['mergeOperations']['secondaryFileKeyCol'], how=jsonObj['mergeOperations']['joinType'])

    for col in jsonObj['removeColsAfterMerge']:
        if col in result.columns:
            result.drop(col, axis=1, inplace=True)

    result.to_csv(jsonObj['mergeOperations']['saveFileAs'], sep=jsonObj['mergeOperations']['mergedFileDelimiter'], index=False, quoting=csv.QUOTE_ALL)

print("########################## SUCCESS ###############################")
############################ Program Ends ##################################
