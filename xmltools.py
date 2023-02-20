import os
import pandas as pd
import logging
from sqlalchemy import create_engine
import db

logging.basicConfig(format='[%(asctime)s] - %(levelname)s: %(message)s', datefmt='%d-%m-%Y %H:%M:%S', level=logging.INFO)

OUTPUT_FOLDER = os.getenv('PATH_OUTPUT_DATA', 'outputs')

def export_xml_content_to_file(xml_content, xml_file_path):
    with open(xml_file_path, 'w') as f:
        content = '\n'.join(xml_content)
        f.write(content)
 

def retrieve_data_from_datalake():
    df_ref = {}
    engine = create_engine(os.getenv('DATALAKE_URL', 'postgresql://postgres:postgres@host.docker.internal:5432/LAFE_PROSTATE_DATALAKE'))
    sqla_schemas = db.reflect_all_schemas()
    for table_name, columns in sqla_schemas.items():
       df_ref[table_name] = pd.read_sql_table(table_name=table_name,con=engine,schema='public', columns=columns.keys())
    return df_ref
 

def export_patients_as_xml():
    # Create output folder if not exists
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
 
    # Retrieve info from datalake
    dataframes = retrieve_data_from_datalake()
    cancer_type = os.getenv('CANCER_TYPE')
    # Process each patient and save it as xml
    for _, patient in dataframes['person'].iterrows():
        logging.info(f"====================================================")
        logging.info(f"processing patient {patient.person_id}")
        xml = ['<?xml version="1.0" encoding="UTF-8"?>']
        xml.append(f'<chaimeleon_{cancer_type} xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="patient_{patient.person_id}.xsd">')
 
        for xml_label, _ in dataframes.items():
            logging.info(f"processing {xml_label} for patient {patient.person_id}")
            # row_label = xml_label.replace("_data", "")
            row_label = xml_label + '_data'
            df_data = dataframes[xml_label][dataframes[xml_label]['person_id']==patient.person_id]
            to_xml = df_data.to_xml(index=False, root_name="data", row_name=row_label, xml_declaration=False, pretty_print=True).split('\n')
            xml += [f'  {x}' for x in to_xml]
 
        xml.append(f'</chaimeleon_{cancer_type}>')
        logging.info(f"====================================================\n")
 
        export_xml_content_to_file(xml, f'{OUTPUT_FOLDER}/patient_{patient.person_id}.xml')