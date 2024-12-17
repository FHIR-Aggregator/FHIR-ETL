import pandas as pd
import json

def convert_to_fhir_subject(input_row):
    ncpi_participant = {
        "resourceType": "Participant",
        "identifier": input_row['Sample name'] if pd.notna(input_row['Sample name']) else "None",
        "meta":{
            "profile": [
                "https://nih-ncpi.github.io/ncpi-fhir-ig-2/StructureDefinition-ncpi-participant.html"
            ]
        },
    }

    extensions = []
    birth_sex = input_row['Sex']
    if pd.notna(birth_sex):
        extensions.append({
            "url": "https://hl7.org/fhir/us/core/STU3.1.1/StructureDefinition-us-core-sex.html",
            "valueString": {
                "coding": [{
                    "birthSex": birth_sex
                }]
            }
        })

    race = input_row['Population name']
    if pd.notna(race):
        extensions.append({
            "url": "https://hl7.org/fhir/us/core/STU3.1.1/StructureDefinition-us-core-race.html",
            "valueString": {
                "coding": [{
                    "race": race
                }]
            }
        })

    population = input_row['Superpopulation name']
    if pd.notna(population):
        extensions.append({
            "url": "https://nih-ncpi.github.io/ncpi-fhir-ig-2/StructureDefinition-research-population.html",
            "valueString": {
                "coding": [{
                    "population": population
                }]
            }
        })

    if extensions:
        ncpi_participant["extension"] = extensions

    return json.dumps(ncpi_participant, indent=4)

def main():
    subject_df = pd.read_csv('igsr_samples.tsv', sep='\t') # obtain following the directions from this folder's readme

    print(subject_df.head(10))
    print("Converting subject df to fhirized json")
    subject_json_strings = []
    for index, row in subject_df.iterrows():
        subject_json_strings.append(convert_to_fhir_subject(row))

    subject_json_dict_list = [json.loads(json_str) for json_str in subject_json_strings]
    with open('subject_fhirized.json', 'w') as f:
        json.dump(subject_json_dict_list, f, indent=4)
    print("Conversion of subject complete, see output dir for subject_fhirized.json")

if __name__ == "__main__":
    main()