import pandas as pd
import numpy as np
import json
import math

###===== GLOBALS =========================
MSG_LOAD_FILES = "Loading old data..."
MSG_UPDATE = "Generating new {table_name} table..."
MSG_SUCCESSFUL = "Done!"

CSV_DELIMITER = '|'
PATH_STR_DELIMITER = '>'
VERSION_ID_OFFSET = 1000000
MEDICAL_CASE_ID_OFFSET = 1000000
PATIENT_ID_OFFSET = 1000000
MIGRATION_ID_OFFSET = 1000000

DEFAULT_FORCE_CLOSE = False
DEFAULT_REDCAP = False
DEFAULT_MC_REDCAP_FLAG = False
DFAULT_LOCAL_MEDICAL_CASE_ID = None
DEFAULT_DUPLICATE = False
DEFAULT_MERGED_WITH = None
DEFAULT_MERGED = False
DEFAULT_STATUS = False
DEFAULT_RELATED_IDS = []
DEFAULT_OTHER_ID = None



###===== FUNCTIONS =======================
def updateUniqueId():
    with open('data/nodes_translations.json', 'r') as file:
        data = file.read()
        file.close()

    data = json.loads(data)
    unique_id = data['unique_id']
    data['unique_id'] = str(int(unique_id) + 1)
    data = json.dumps(data, indent=4)

    with open('data/nodes_translations.json', 'w') as file:
        file.write(data)
        file.close()

    return int(unique_id)

def read_csv(file_name):
    return pd.read_csv(file_name, delimiter=CSV_DELIMITER)

def read_json(file_name):
    file = open(file_name,)
    data = json.load(file)
    file.close()

    return data

def getPatientVersionId(patient_id):
    version_id = data_medical_cases.loc[data_medical_cases['patient_id'] == patient_id]
    if(version_id.empty):
        return None
    version_id = version_id.iloc[0]
    version_id = version_id['version_id']

    return version_id

def valueToStr(value):
    if(isinstance(value, bool)):
        return 'true' if value else 'false'

    if(value is None):
        return ''

    if(isinstance(value, float) and math.isnan(value)):
        return ''
    
    return str(value)

def valuesToRow(values):
    row = str(values[0])
    for value in values[1:]:
        row += CSV_DELIMITER
        row += valueToStr(value)
    row += "\n"
    
    return row

def findKeyInJson(key_to_find, json, current_path, paths, flexible):
    if not isinstance(json, dict):
        return set()

    for key in json.keys():
        new_path = current_path + PATH_STR_DELIMITER + key
        if((key == key_to_find and not flexible) or (key_to_find.lower() in key.lower() and flexible)):
            paths.add(new_path)

        findKeyInJson(key_to_find, json[key], new_path, paths, flexible)
        
    return paths

def getPatientNodeValue(key, version_id, patient_id):
    data = data_json[str(version_id)]
    if key not in data:
        return None

    node_id = data[key]
    value = data_patient_values.loc[data_patient_values['patient_id'] == patient_id]
    value = value.loc[value['node_id'] == int(node_id)]
    if(value.empty):
            return None
    else:
        value = value.iloc[0]
        value = value['value']
        return value

def loadData():
    print(MSG_LOAD_FILES)

    # Unique id to get new ids in database
    unique_id = updateUniqueId()

    data_versions = read_csv('data/versions.csv')
    data_medical_cases = read_csv('data/medical_cases.csv')
    data_patients = read_csv('data/patients.csv')
    data_patient_values = read_csv('data/patient_values.csv')

    data_json = read_json('data/nodes_translations.json')

    print(MSG_SUCCESSFUL)

    return unique_id, data_versions, data_medical_cases, data_patients, data_patient_values, data_json

def updateTable(table_name, updateFunction):
    print(MSG_UPDATE.format(table_name=table_name))

    with open(f'new_data/{table_name}.csv', 'w') as new_table:
        updateFunction(new_table)

    print(MSG_SUCCESSFUL)

def updateVersions(new_table):
    for index, row in data_versions.iterrows():
        data_json = row['json']
        data_json = data_json.replace("\'\"", "\"")
        data_json = json.loads(data_json)

        values = []
        # id
        values.append(row['id'] + unique_id * VERSION_ID_OFFSET)

        # medal_c_id
        values.append(row['version_medal_c_id'])

        # name
        values.append(data_json['version_name'])

        # algorithm_id
        values.append(data_json['algorithm_id'])

        # created_at
        values.append(row['created_at'])

        # updated_at
        values.append(row['updated_at'])

        # consent_management
        values.append(data_json['config']['consent_management'])

        # study
        values.append(data_json['study']['label'])

        new_row = valuesToRow(values)
        new_table.write(new_row)

def updateMedicalCases(new_table):
    for index, row in data_medical_cases.iterrows():
        data_json = row['case']
        data_json = data_json.replace("\'\"", "\"")
        data_json = json.loads(data_json)

        values = []
        # id
        values.append(row['id'] + unique_id * MEDICAL_CASE_ID_OFFSET)

        # version_id
        values.append(row['version_id'])

        # patient_id
        values.append(int(row['patient_id'] + unique_id * PATIENT_ID_OFFSET))

        # created_at
        values.append(row['created_at'])

        # updated_at
        values.append(row['updated_at'])

        # local_medical_case_id
        values.append(row['uuid'])

        # consent
        data_patient = data_json['patient']
        values.append('consent_file' in data_patient and data_patient['consent_file'] is not None)

        # isEligible
        values.append(data_json['isEligible'])

        # group_id
        data_patient = data_json['patient']
        group_id = data_patient['group_id'] if 'group_id' in data_patient.keys() else None
        values.append(group_id)
            
        # redcap
        values.append(DEFAULT_REDCAP)
        # consultation_date
        values.append(row['updated_at'])
        # closed_at
        values.append(row['updated_at'])
        # force_close 
        values.append(DEFAULT_FORCE_CLOSE)
        # mc_redcap_flag
        values.append(DEFAULT_MC_REDCAP_FLAG)

        new_row = valuesToRow(values)
        new_table.write(new_row)

def updatePatients(new_table):
    for index, row in data_patients.iterrows():
        patient_id = row['id']

        version_id = getPatientVersionId(patient_id)
        if(version_id is None):
            continue

        values = []
        # id
        values.append(patient_id + unique_id * PATIENT_ID_OFFSET)

        # first_name
        first_name = getPatientNodeValue('first_name', version_id, patient_id)
        values.append(first_name)

        # last_name
        last_name = getPatientNodeValue('last_name', version_id, patient_id)
        values.append(last_name)

        # created_at
        values.append(row['created_at'])

        # updated_at
        values.append(row['updated_at'])

        # birthdate
        birth_date = getPatientNodeValue('birth_date', version_id, patient_id)
        values.append(birth_date)

        # weight
        weight = getPatientNodeValue('weight', version_id, patient_id)
        values.append(weight)

        # gender
        gender = getPatientNodeValue('gender', version_id, patient_id)
        if(gender == "394"):
            gender = 'male'
        if(gender == "393"):
            gender = 'female'
        values.append(gender)

        # local_patient_id
        values.append(row['uuid'])

        # group_id
        values.append(row['group_id'])

        # consent
        values.append(row['consent_file'] is not None)

        # redcap
        values.append(DEFAULT_REDCAP)

        # duplicate
        values.append(DEFAULT_DUPLICATE)

        # other_uid
        values.append(row['other_uid'])

        # other_study_id
        values.append(row['other_study_id'])

        # other_group_id
        values.append(row['other_group_id'])

        # merged_with
        values.append(DEFAULT_MERGED_WITH)

        # merged
        values.append(DEFAULT_MERGED)

        # status
        values.append(DEFAULT_STATUS)

        # related_ids
        values.append(DEFAULT_RELATED_IDS)

        # middle_name
        values.append(None)
        middle_name = getPatientNodeValue('middle_name', version_id, patient_id)
        values.append(middle_name)

        # other_id
        values.append(DEFAULT_OTHER_ID)

        new_row = valuesToRow(values)
        new_table.write(new_row)

###===== MAIN ============================
unique_id, data_versions, data_medical_cases, data_patients, data_patient_values, data_json = loadData()

updateTable('versions', updateVersions)
updateTable('medical_cases', updateMedicalCases)
updateTable('patients', updatePatients)