import pandas as pd
import os
from psycopg2 import Error
import configuration
import csv

# To Do
# make it dynamic for all resource files - currently working for the Heritage Resource Model

data_dir = os.path.join(os.getcwd(), "../Data")
print (data_dir)
file_path = os.path.join(data_dir, "MAHSAHeritageResourceModel.csv")
rl_path = os.path.join(data_dir, "Related Resources.csv")
rm_name_lookup = {'HRG': 'Heritage Resource Group Resource Model', 'HR': 'Heritage Resource Model',
                  'AC': 'Actor Resource Model','ACT': 'Activity Resource Model',
                  'IR': 'Information Resource Model', 'HM': 'Historical Maps Resource Model','E': 'Grid Resource Model'}

def convert_split_csv():
    """
    This function takes a multiple sheet xlsx files as input and saves the individual sheets as csv files
    Args:
    Returns:
    Raises:
    """
    # read only the master xlsx files from the folder
    files = os.listdir(data_dir)
    files_xlsx = [f for f in files if f[-4:] == 'xlsx']
    for file in files_xlsx:
        xl = pd.ExcelFile(os.path.join(data_dir, file))
        for sheet in xl.sheet_names:
            # skip top two rows of
            if sheet == 'Related Resources':
                df = pd.read_excel(xl,sheet_name=sheet)
            else:
                df = pd.read_excel(xl, sheet_name=sheet, skiprows=2)
            # if sheet != 'Related Resources':
            #     df = df.iloc[2:]
            cols = df.columns.tolist()
            cols = cols[:1] + cols[10:] + cols[1:10]
            df = df[cols]
            path = data_dir +'\\'+ sheet
            df.to_csv(f"{path}.csv",index=False,quoting=csv.QUOTE_ALL)
            # df.to_excel(f"{path}.xlsx",index=False) # this would remove the formatting though
            print(sheet, 'Saved as a separate file')

def get_file_list(dir,ext):
    """This function returns the the list of specific extension files in a folder

    Args:
        dir: the data directory where files exist
        ext: the extension of files to return

    Returns: list of specific extension files

    """
    files=[]
    for file in os.listdir(dir):
        if file.endswith(ext):
            files.append(file)
    return files

def get_mahsaid_columns(file_path):
    """
    This function reads the csv file and looks for the columns that contain ID in their name

    Returns: The list of ID Columns
    """
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

def process_standard_relations(rl_path):
    """This function process the standard relations file

    Args:
        rl_path (string): path of the standard relations file.

    """
    rm_dict = {}
    missing_map_files = []
    for file in get_file_list(data_dir,'.mapping'):
        read_file = open(os.path.join(data_dir,file), 'r')
        for line in read_file:
           if line.split(':')[0].strip() == '"resource_model_name"':
                key = line.split(':')[1].strip().replace('"','').replace(',','')
           if line.split(':')[0].strip() == '"resource_model_id"':
                value = line.split(':')[1].strip().replace('"','',).replace(',','')
        rm_dict[key] = value
    rl_df = pd.read_csv(rl_path)

    for index, row in rl_df.iterrows():
        for col in ['resourceinstanceidfrom', 'resourceinstanceidto']:
            rl_df.loc[index, col+'_graphname'] = rm_name_lookup[str(row[col])[:2]]

        if rl_df.loc[index,'resourceinstanceidfrom_graphname'] in rm_dict.keys():
            rl_df.loc[index, 'resourceinstancefrom_graphid'] = rm_dict[rl_df.loc[index,'resourceinstanceidfrom_graphname']]
        elif rl_df.loc[index, 'resourceinstanceidfrom_graphname'] not in missing_map_files:
            missing_map_files.append(rl_df.loc[index, 'resourceinstanceidfrom_graphname'])

        if rl_df.loc[index, 'resourceinstanceidto_graphname'] in rm_dict.keys():
            rl_df.loc[index, 'resourceinstanceto_graphid'] = rm_dict[rl_df.loc[index, 'resourceinstanceidto_graphname']]
        elif rl_df.loc[index, 'resourceinstanceidto_graphname'] not in missing_map_files:
            missing_map_files.append(rl_df.loc[index, 'resourceinstanceidto_graphname'])

    if len(missing_map_files) > 0:
        print(f"Following mapping file are missing'{missing_map_files}', resource relations not processed",)
    else:
        rl_df.to_csv(os.path.join(data_dir, "RelatedResource_Processed.csv"), index=False)
        print('--Resource relations processed--')

if __name__ == '__main__':
    convert_split_csv()
  # populate_resource_instance_relations()
    print('Now processing the standard resource relations file')
    process_standard_relations(rl_path)