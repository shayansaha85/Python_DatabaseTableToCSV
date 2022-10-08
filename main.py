from jproperties import Properties
import mysql, mysql.connector
import pandas as pd

def sql2csv(sqlfile, savefile):
    # READING QUERY FROM SQL FILE
    file = open(sqlfile, "r")
    query = file.read()
    file.close()
    query = query.strip()

    # READING DATABASE INFORMATION FROM PROPERTIES FILE
    config = Properties()
    with open('database.properties', 'rb') as read_prop:
        config.load(read_prop)
    
    # CONNECTING DATABASE AND EXECUTING QUERY
    db = mysql.connector.connect(
        host = config.get("db_host").data,
        user = config.get("db_username").data,
        passwd = config.get("db_password").data,
        database = config.get("db_name").data
    )
    cursor = db.cursor(dictionary=True)
    cursor.execute(query)
    result = cursor.fetchall()

    # FINDING COLUMN NAMES
    length_data = len(result)
    if length_data > 0 :
        columnNames = []
        for key in result[0]:
            columnNames.append(key)
        print("COLUMN NAMES :",columnNames)
        main_data = {}
    
        # PREPARING DATA DICTIONARY FOR DATAFRAME
        for k in columnNames:
            main_data[k] = []

        # ADDING DATA IN DATA DICTIONARY
        for x in result:
            for k in columnNames:
                main_data[k].append(x[k])
    
        # GENERATING CSV SHEET
        df = pd.DataFrame(main_data)
        df.to_csv(savefile, sep="|", index=False)
        print("**** CSV GENERATED ****")

    else:
        print("**** NO DATA FETCHED FOR THE QUERY PASSED ****")

sql2csv("./sample_query.sql", "./output/employeeDetails.csv")