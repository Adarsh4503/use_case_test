import logging
class file_extension_check:
    def check_ext(ti):
        file_name=ti.xcom_pull(key='return_value', task_ids=['return_file_name'])
        if(file_name[0].endswith('.xlsx')):
            logging.info("Is an excel file ")
            return True
        else:
            logging.error('Not the required file format')
            return False
    
