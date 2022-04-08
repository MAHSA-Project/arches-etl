import pandas as pd
import os
from psycopg2 import Error
import configuration

# To Do
# make it dynamic for all resource files - currently working for the Heritage Resource Model

cwd = os.getcwd()
data_dir = os.path.join(os.getcwd(), "../Data")
print (data_dir)
file_path = os.path.join(os.getcwd(), "../Data/MAHSAHeritageResourceModel.csv")

def convert_split_csv():
    # read only the master xlsx files from the folder
    files = os.listdir(data_dir)
    files_xlsx = [f for f in files if f[-4:] == 'xlsx']
    for file in files_xlsx:
        xl = pd.ExcelFile(os.path.join(data_dir, file))
        for sheet in xl.sheet_names:
            # skip top two rows of
            df = pd.read_excel(xl,sheet_name=sheet, skiprows=2)
            # if sheet != 'Related Resources':
            #     df = df.iloc[2:]
            cols = df.columns.tolist()
            cols = cols[:1] + cols[10:] + cols[1:10]
            df = df[cols]
            path = data_dir +'\\'+ sheet
            df.to_csv(f"{path}.csv",index=False)
            # df.to_excel(f"{path}.xlsx",index=False) # this would remove the formatting though
            print(sheet, 'Saved as a separate file')

def get_file_list():
    return (os.listdir(data_dir))

def get_mahsaid_columns(file_path):
    # get list of all columns to be used in resource instance relations
    df = pd.read_csv(file_path)
    id_columns = list(df.filter(regex='ID').head().columns)
    id_columns.remove('ResourceID')
    id_columns.remove('MAHSA_ID')
    return (id_columns)

def populate_resource_instance_relations():
    try:
        # Connect to postgres database
        connection = configuration.connect_postgres()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

    # Create a cursor to perform database operations
    cursor = connection.cursor()
    # Executing a SQL query
    cursor.execute(configuration.query_str)
    # Fetch result
    record = cursor.fetchall()
    id_columns = get_mahsaid_columns(file_path)
    df = pd.read_csv(file_path)
    for column in id_columns:
        for index, row in df.iterrows():
            if str(row[column])!='nan':
                for data in record:
                    if str(row[column]) == data[1]:
                        resource_instance_list = []
                        resource_instance_dict = dict.fromkeys(['resourceId', 'ontologyProperty', 'resourceXresourceId', 'inverseOntologyProperty'])
                        resource_instance_dict['resourceId'] = data[0]
                        resource_instance_dict['ontologyProperty'] = row[df.columns.get_loc(column) + 1]
                        resource_instance_dict['inverseOntologyProperty'] = row[df.columns.get_loc(column) + 2]
                        resource_instance_dict['resourceXresourceId'] = ''
                        resource_instance_list.append(resource_instance_dict)
                        df.loc[index, str.replace(column, ' ID','')] = str(resource_instance_list)
    df.to_csv(os.path.join(os.getcwd(), "../Data/MAHSAHeritageResourceModel_updated.csv"), index=False)
    print('Resource instance relationships have been populated and output file is saved')

convert_split_csv()

populate_resource_instance_relations()