from google.cloud import bigquery
import pandas as pd
import constant
import logging

class bq_load:
    
    def insert_func(ti):
        project_name=constant.PROJECT_NAME      #name of the active project in GCP
        dataset_id = constant.DATASET_ID        #name of the dataset where the target table lies        #VALUES CAN BE FOUND IN CONSTANTS FOLDER
        table_name=constant.TABLE_NAME          #name of the table where the data is to be uploaded
       
        client=bigquery.Client()                
        table_id=project_name+'.'+dataset_id+'.'+table_name   #each table in BQ is refernced using the format - `project_name.dataset_name.table_name`

        #returns the filename of the file which triggered the cloud function, this was pushed to xcoms 
        file_name=ti.xcom_pull(key='return_value', task_ids=['return_file_name']) 
       
        #location of the uploaded file in the GCS bucket
        uri = 'gs://' + 'test_bucket_69420'+ '/' + file_name[0]     

         #reading the xlsx file from the GCS bucket with the uri and setting the header as the second column of the excel file
        df=pd.read_excel(uri,header=[1])  


        #START
        #filling the null values with appropriate default values
        df[['Area code','Sub Category','Category']]=(df.loc[:,['Area code','Sub Category','Category']]).fillna('') 
        df[['Feature Factors','Unnamed: 6']]=(df.loc[:,['Feature Factors','Unnamed: 6']]).fillna(0)
        df[['Multiplying Factor']]=(df.loc[:,['Multiplying Factor']]).fillna(1)
        df[['Unnamed: 7']]=(df.loc[:,['Unnamed: 7']]).fillna('0,0')
        #END

        rows_to_insert=[]

        #START
        #reading the excel data row by row an appending it to a list of json values with a fixed schema
        for i in range (1,df.shape[0]):
            x=df.iloc[i,:]              # x storeing row data (it changes per run of the loop)
            rows_to_insert.append({     
            "Contributor": x[0],
            "Sub_category": x[1],
            "Category": x[2],
            "Multiplying_factor": int(x[3]),
            "Area_code": x[4],
            "Feature_Factors": {
                "Feature_1": x[5],
                "Feature_2": x[6],
                "Feature_3": [(x[7].split(','))[0],(x[7].split(','))[1]]    #splitting Feature_3 wherever there is comma.
                                }
                    })


        errors = client.insert_rows_json(table_id, rows_to_insert, row_ids=[None] * len(rows_to_insert))  # Make an API request.
        if errors == []:
            logging.info("New rows have been added.")
        else:
            logging.error("Encountered errors while inserting rows: {}".format(errors))


bq_load.insert_func.__doc__="""A function that takes filename of the uploaded file which cased the cloud function trigger, 
                            then transform that file into a pandas datafranme , extracts the value line by line , saves it to a list of json values 
                            and then performs a batch upload into BigQuery """


