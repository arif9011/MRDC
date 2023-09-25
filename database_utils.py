import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect

import pandas as pd
class DatabaseConnector:
    # task3 step2
    def read_db_creds(self):
       with open('db_creds.yaml', 'r') as db_creds_file:
            db_creds = yaml.safe_load(db_creds_file)
            #print(db_creds)
            print(db_creds)
            return db_creds

     #task3 step3
    def init_db_engine(self):
        db_creds = self.read_db_creds()
        engine = create_engine(f"postgresql://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        print(engine.connect)
        return engine.connect()
   
   
    #task3 step4
    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()
         
     #task3 step7
    def upload_to_db(self,df,table_name):  
        
        with open('creds.yaml', 'r') as creds_file:
            creds = yaml.safe_load(creds_file)
            print(creds)
            
        local_engine = create_engine(f"postgresql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}")
        #local_engine = create_engine(f"postgresql://{creds['PASSWORD']}@localhost:5432/sales_data")
        print('local_engine.connect')
        
       
        df.to_sql(table_name, local_engine, if_exists='replace')
        
        
        
if __name__== "__main__":
   
    db_connector = DatabaseConnector()
    instance = DatabaseConnector()
    db_connector.read_db_creds()
    db_connector.init_db_engine()
    table=db_connector.list_db_tables()
    print(table)
    db_connector.upload_to_db()
   

