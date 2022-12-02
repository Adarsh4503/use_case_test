import pandas as pd
import logging

class schema_valid:
    

    def validation(ti):
        file_name=ti.xcom_pull(key='return_value', task_ids=['return_file_name'])
        uri = 'gs://' + 'test_bucket_69420'+ '/' + file_name[0]
        df=pd.read_excel(uri,header=[1],nrows=0)
        xl_columns=[]
        xl_columns=df.columns
        count=0
        flag=0
        schema_columns=['Contributor', 'Sub Category', 'Category', 'Multiplying Factor','Area code', 'Feature Factors', 'Unnamed: 6', 'Unnamed: 7']

        try:
            for i in xl_columns:
                if i!=schema_columns[count]:
                    flag=1
                count+=1
        
        except Exception:
            logging.critical("Unknown Schema or Null headers")
            return False 
            
            
        if flag ==1:
            logging.error("Invalid Schema")
            return False 
            
        else:
            logging.info("Schema match")
            return True
            


        