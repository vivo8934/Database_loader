import sys
import os
import glob
import json
import pandas as pd
from operator import itemgetter



def get_column_names(schemas, ds_name, sorting_key='column_position'):
    if ds_name not in schemas:
        raise KeyError(f"Dataset '{ds_name}' not found in schemas.")
    column_details = schemas[ds_name]
    if not all(sorting_key in col for col in column_details):
        raise ValueError(f"Some columns in dataset '{ds_name}' are missing the sorting key '{sorting_key}'.")
    columns = sorted(column_details, key=itemgetter(sorting_key))
    #columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

def read_csv(file, schemas):
    if not os.path.exists(file):
        raise FileNotFoundError(f"CSV file '{file}' not found.")
    ds_name = os.path.basename(os.path.dirname(file))
    columns = get_column_names(schemas, ds_name)
    df_reader = pd.read_csv(file, names=columns,chunksize=10000)
    return df_reader

def to_sql(df,db_conn_uri,ds_name):
    df.to_sql(
        ds_name,
        db_conn_uri,
        if_exists='append',
        index=True
    )

def db_loader(src_base_dir,db_conn_uri,ds_name):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
    if len(files) == 0:
        raise NameError(f'No files found for {ds_name}')
    
    for file in files:
        df_reader = read_csv(file,schemas)
        for idx, df in enumarate(df_reader):
            print(f'Populating chunk {idx} of {ds_name}')
            to_sql(df,db_conn_uri,ds_name)

def process_files(ds_names=None):
    src_base_dir = os.environ.get('SRC_BASE_DIR')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')
    db_conn_uri = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    if not ds_name:
        ds_names = schemas.keys()
    for ds_name in ds_names:
        try:
            print(f'Processing {ds_name}')
            db_loader(src_base_dir,db_conn_uri,ds_name)
        except NameError as ne:
            print(ne)
            pass
        except Exception as e:
            print(e)
            pass
        finally:
            print(f'Processing Error in {ds_name}')

if __name__ == '__main__':
    if len(sys.argv == 2):
        ds_name = json.loads(sys.argv[1])
        process_files(ds_names)
    else:
        process_files()
