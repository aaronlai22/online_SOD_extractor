from sqlalchemy import create_engine, inspect, Table, MetaData, Column, Integer, String, exc
from sqlalchemy.orm import declarative_base, sessionmaker
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
import os, glob, time, logging, psycopg2
from psycopg2 import sql

# Base directory containing the XML files
base_directory = r'\\S33file1.kssm.intel.com\sdx\datalogs\1276\prod'

# Database credentials
username = ''
password = ''
host = 'postgres5184-lb-pg-in.iglb.intel.com'
port = '5433'
database = 'db_ai'
strSchema = 'NetAppSOD'
strTable = 'SOD_data'

# Create the connection string
connection_string = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'

# For test purpose, put a lot number , example - 'D43591001', else put '*' for live testing
strLot_Lots = '*'

class Logger:
    def __init__(self, log_file='application.log'):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # Create a file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)

        # Create a console handler (optional, for console output)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Create a logging format
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add the handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def get_logger(self):
        return self.logger

# Create a base class for declarative class definitions
Base = declarative_base()

# Define a sample table with the specified fields
class Record(Base):
    __tablename__ = strTable
    __table_args__ = {'schema': strSchema} 
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    Lot = Column(String)
    Operation = Column(String)
    Wafer = Column(String)
    IB = Column(String)
    Count = Column(Integer)

class DatabaseManager:
    def __init__(self, connection_string):
        # Initialize the logger
        logger_instance = Logger()
        self.logger = logger_instance.get_logger()
        
        # Create a new SQLAlchemy engine
        self.engine = create_engine(connection_string, echo=False)
        
        # Create a sessionmaker
        self.Session = sessionmaker(bind=self.engine)

        # Ensure the schema and table exist
        self.ensure_schema_and_table()

    def ensure_schema_and_table(self):
        # Connect using psycopg2 to create schema if it doesn't exist
        connpsycopg2 = psycopg2.connect(dbname=database, user=username, password=password, host=host, port=port)
        cursor = connpsycopg2.cursor()
        
        try:
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(strSchema)))
            connpsycopg2.commit()
            print(f"Schema '{strSchema}' has been created or already exists.")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            cursor.close()
            connpsycopg2.close()

        # Create the table in the database if it doesn't exist
        Base.metadata.create_all(self.engine)

    def read_records(self):
        # Create a new session
        session = self.Session()

        try:
            # Query all records
            records = session.query(Record).all()
            for record in records:
                print(f"ID: {record.id}, Lot: {record.Lot}, Operation: {record.Operation}, "
                      f"Wafer: {record.Wafer}, IB: {record.IB}, Count: {record.Count}")
        except exc.SQLAlchemyError as e:
            print(f"An error occurred: {e}")
        finally:
            # Close the session
            session.close()
            
    def insert_records(self, records):
        # Insert multiple records into the database only if they do not already exist.
        session = self.Session()
        try:
            for record in records:
                # Check if the record already exists
                existing_record = session.query(Record).filter_by(
                    Lot=record.Lot,
                    Operation=record.Operation,
                    Wafer=record.Wafer,
                    IB=record.IB
                ).first()

                if not existing_record:
                    # Add the record to the session if it does not exist
                    session.add(record)
                    self.logger.info(f"Inserted new record: Lot={record.Lot}, Operation={record.Operation}, Wafer: {record.Wafer}, IB: {record.IB}, Count: {record.Count}")
                else:
                    self.logger.info(f'Record already exists - Lot={record.Lot}, Operation={record.Operation}, Wafer={record.Wafer}, IB={record.IB}, Count: {record.Count}')

            # Commit the transaction
            session.commit()
            self.logger.info("All records have been processed and committed.")

        except exc.SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"An error occurred during record insertion: {e}")
        finally:
            session.close()
            self.logger.info("Session closed after record insertion.")
    
    def bulk_insert_from_dataframe(self, session, df, model):
        """
        Perform a bulk insert of records from a DataFrame into the database.

        :param session: SQLAlchemy session object
        :param df: DataFrame containing the data to be inserted
        :param model: SQLAlchemy model class representing the table
        """
        try:
            # Convert DataFrame to a list of dictionaries
            records = df.to_dict(orient='records')

            # Perform bulk insert
            session.bulk_insert_mappings(model, records)
            session.commit()
            print(f"Successfully inserted {len(records)} records into the database.")
            
        except exc.SQLAlchemyError as e:
            session.rollback()
            print(f"An error occurred during bulk insertion: {e}")

        finally:
            session.close()
            print("Session closed after bulk insertion.")
            
class XMLFileHandler:
    def __init__(self, xml_file):
        self.xml_file = xml_file

    def parse_xml(self):
        tree = ET.parse(self.xml_file)
        root = tree.getroot()
        data = []
        for t_element in root.findall('T'):
            t_data = {}
            for child in t_element:
                if child.tag in ['TrayTSD', 'TrayTED']:
                    t_data[child.tag] = datetime.strptime(child.text, '%Y%m%d%H%M%S').strftime('%Y/%m/%d %H:%M:%S')
                else:
                    t_data[child.tag] = child.text
            data.append(t_data)
        return data

class LotProcessor:
    def __init__(self, base_directory, lot_number, strOperation):
        self.base_directory = base_directory
        self.lot_number = lot_number
        self.strOperation = strOperation
        self.all_data = []
        self.check_data_list = []
        
    def generate_full_paths(self):
        # Use regular expression - to find any lot_number
        return [os.path.join(self.base_directory, f"{self.lot_number}_{postfix}", 'SOD_UPLOAD') for postfix in self.strOperation]
    
    def prelim_check_operations(self):
        pre_check_ops_paths = list()
        
        full_paths = self.generate_full_paths()
        for path in full_paths:
            if path not in pre_check_ops_paths:
                pre_check_ops_paths.append(path)
                
        return pre_check_ops_paths
    
    def get_fulfilled_lots(self):
        fulfilled_lots = []
        for path in self.generate_full_paths():
            if os.path.exists(path):
                xml_files = glob.glob(os.path.join(path, '*.xml'))
                if any('SORTBIN' in os.path.basename(xml_file) for xml_file in xml_files):
                    fulfilled_lots.append(self.lot_number)
                    break
        return fulfilled_lots

    def process_lot(self):
        print(f'Processing lot begins...')
        
        # Initialize the logger
        logger_instance = Logger()
        logger = logger_instance.get_logger()
        
        # Create a set to track unique entries
        existing_data_set = set(tuple(d.items()) for d in self.all_data)
        
        for postfix in self.strOperation:
            full_paths = self.generate_full_paths()
            
            for path in full_paths:
                xml_files = glob.glob(os.path.join(path, '*.xml'))
                if not xml_files:
                    print(f"The list of XML files are not found in: {path}")
                    continue
                
                for xml_file in xml_files:
                    if os.path.exists(xml_file) and 'SORTBIN' in os.path.basename(xml_file):
                        print(f'{time.ctime(time.time())} - Processing file: {xml_file}')
                        handler = XMLFileHandler(xml_file)
                        xml_parsed_data = handler.parse_xml()
                        
                        print(f'{xml_file} already parsed.')                        
                            
                        # Add only unique entries
                        for data in xml_parsed_data:
                            data_tuple = tuple(data.items())
                            if data_tuple not in existing_data_set:
                                self.all_data.append(data)
                                existing_data_set.add(data_tuple)
                    else:
                        print(f"XML file not exist: {xml_file}.")
                        logger.warning(f"XML file not exist: {xml_file}.")
                        
        print(f'\n  -------------------------------------------')
        print(f'   Entire process_lot function is completed.')
        print(f'  -------------------------------------------')

        # Convert the list of dictionaries to a DataFrame
        df = pd.DataFrame(self.all_data)
        return df

    def generate_pivot_table(self, dataframe):
        if not dataframe.empty:
            return pd.pivot_table(dataframe, index=['Lot', 'Operation', 'Wafer', 'IB'], aggfunc='size').reset_index(name='Count')
        return pd.DataFrame()

    def merge_data(self, pivot_table):
        if self.check_data_list:
            check_df = pd.DataFrame(self.check_data_list)
            check_df = check_df.replace('', 'NaN')
            return pd.concat([check_df, pivot_table], ignore_index=True)
        return pivot_table

def main():
    start_time = time.time()
    
    db_manager = DatabaseManager(connection_string)
    
    # Initialize the logger
    logger_instance = Logger()
    logger = logger_instance.get_logger()
         
    strOperation = ['119325', '132110', '132150']
    
    process_avail_lot = LotProcessor(base_directory, strLot_Lots, strOperation)
    
    print('Please wait... Seeking all the available lots, before processing.')
    
    # Get the DataFrame from process_lot
    df_temp = process_avail_lot.process_lot()

    print(f'\nPivoting the data frame, please wait...')
    pivot_table = process_avail_lot.generate_pivot_table(df_temp)
    # print(pivot_table)    
    
    # Create Record objects for batch insertion
    records_temp = [
        Record(
            Lot = row['Lot'],
            Operation = row['Operation'],
            Wafer = row['Wafer'],
            IB = row['IB'],  
            Count = row['Count']
        )
        for index, row in pivot_table.iterrows()
    ]

    db_manager.insert_records(records_temp)
    
    stop_time = time.time()
    
    elapsed_time = stop_time - start_time
    
    # Convert seconds to hours, minutes, and seconds
    hours = elapsed_time // 3600
    minutes = (elapsed_time % 3600) // 60
    seconds = elapsed_time % 60

    print(f"Start Time: {time.ctime(start_time)}")
    logger.info(f"Start Time: {time.ctime(start_time)}")
    
    print(f"Stop Time: {time.ctime(stop_time)}")
    logger.info(f"Stop Time: {time.ctime(stop_time)}")
    
    print(f"Elapsed Time: {hours} hours, {minutes} minutes, {seconds:.2f} seconds")
    logger.info(f"Elapsed Time: {hours} hours, {minutes} minutes, {seconds:.2f} seconds")

def Read_main():
    db_manager = DatabaseManager(connection_string)

    print("\nReading all records:")
    db_manager.read_records()

if __name__ == "__main__":
    main()