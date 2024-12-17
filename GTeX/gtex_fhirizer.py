import pandas as pd
import json
import requests

def retrieve_paginated_gtex_data(api_endpoint):
    page = 0
    all_data = []
    response = requests.get(api_endpoint, params={'datasetId': 'gtex_v10', 'itemsPerPage': 100, 'page': 0}).json()
    max_pages = response['paging_info']['numberOfPages'] # 436 for sample
    print(f"Aggregating {api_endpoint} data through a total of {max_pages} pages")
    print(f"Page {page}")
    all_data.extend(response['data'])

    while page < max_pages:
        page += 1
        print(f"Page {page}")
        next_response = requests.get(api_endpoint, params={'datasetId': 'gtex_v10', 'itemsPerPage': 100, 'page': page}).json()
        all_data.extend(next_response['data'])

    return all_data

def convert_to_fhir_subject(input_row):
    ncpi_participant = {
        "resourceType": "Participant",
        "identifier": input_row['subjectId'],
        "meta":{
            "profile": [
                "https://nih-ncpi.github.io/ncpi-fhir-ig-2/StructureDefinition-ncpi-participant.html"
            ]
        },
        "birthDate": f"{2024 - int(input_row['ageBracket'].split('-')[1])} - {2024 - int(input_row['ageBracket'].split('-')[0])}", # age is displayed in the form of 60-69 in input phenotype data as an example. Final year estimate should look like 1964 - 1975.
        "deceased": "Yes" if pd.notna(input_row['hardyScale']) else "No"
    }

    extensions = []
    birth_sex = input_row['sex']
    if pd.notna(birth_sex):
        extensions.append({
            "url": "https://hl7.org/fhir/us/core/STU3.1.1/StructureDefinition-us-core-sex.html",
            "valueString": {
                "coding": [{
                    "system": "https://storage.googleapis.com/adult-gtex/annotations/v10/metadata-files/GTEx_Analysis_v10_Annotations_SubjectPhenotypesDD.xlsx",
                    "birthSex": birth_sex
                }]
            }
        })

    death_circumstance = input_row['hardyScale']
    if pd.notna(death_circumstance):
        extensions.append({
            "url": "https://hl7.org/fhir/R4B/extension-condition-dueto.html",
            "valueString": {
                "coding": [{
                    "system": "https://storage.googleapis.com/adult-gtex/annotations/v10/metadata-files/GTEx_Analysis_v10_Annotations_SubjectPhenotypesDD.xlsx",
                    "deathCircumstance": death_circumstance
                }]
            }
        })

    if extensions:
        ncpi_participant["extension"] = extensions

    return json.dumps(ncpi_participant, indent=4)

def convert_to_fhir_sample(input_row):
    ncpi_sample = {
        "resourceType": "Sample",
        "identifier": input_row['aliquotId'],
        "meta":{
            "profile": [
                "https://nih-ncpi.github.io/ncpi-fhir-ig-2/StructureDefinition-ncpi-sample.html"
            ]
        },
        "type": input_row['dataType'] if pd.notna(input_row['dataType']) else "Not specified",
        "subject": input_row['subjectId'] if pd.notna(input_row['subjectId']) else "Not specified",
        "parent": input_row['sampleId'],
        "collection":{
            "method": input_row['freezeType'],
            "bodySite": input_row['tissueSiteDetail']
        },
        "processing":{
            "procedure": input_row['freezeType']
        },
        "dataset": input_row["datasetId"]
    }

    return json.dumps(ncpi_sample, indent=4)

def main():
    subject_endpoint = "https://gtexportal.org/api/v2/dataset/subject"
    sample_endpoint = "https://gtexportal.org/api/v2/dataset/sample"

    subject_data = retrieve_paginated_gtex_data(subject_endpoint)
    sample_data = retrieve_paginated_gtex_data(sample_endpoint)

    subject_df = pd.DataFrame(subject_data)
    sample_df = pd.DataFrame(sample_data)

    print(subject_df.head(10))
    print("Converting subject df to fhirized json")
    subject_json_strings = []
    for index, row in subject_df.iterrows():
        subject_json_strings.append(convert_to_fhir_subject(row))

    subject_json_dict_list = [json.loads(json_str) for json_str in subject_json_strings]
    with open('subject_fhirized.json', 'w') as f:
        json.dump(subject_json_dict_list, f, indent=4)
    print("Conversion of subject complete, see output dir for subject_fhirized.json")

    print(sample_df.head(10))
    print("Converting sample df to fhirized json")
    sample_json_strings = []
    for index, row in sample_df.iterrows():
        sample_json_strings.append(convert_to_fhir_sample(row))

    sample_json_dict_list = [json.loads(json_str) for json_str in sample_json_strings]
    with open('sample_fhirized.json', 'w') as f:
        json.dump(sample_json_dict_list, f, indent=4)
    print("Conversion of sample complete, see output dir for sample_fhirized.json")

if __name__ == "__main__":
    main()