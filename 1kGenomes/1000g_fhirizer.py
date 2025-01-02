from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import json
import os
from pathlib import Path
import shutil

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
    DOWNLOADS = Path.home() / 'Downloads'
    print(DOWNLOADS)

    if os.path.isfile(f"{DOWNLOADS}/igsr_samples.tsv"):
        os.remove(f"{DOWNLOADS}/igsr_samples.tsv")

    print("Using selenium w/ Firefox browser to obtain sample file")
    driver = webdriver.Firefox()
    driver.get('https://www.internationalgenome.org/data-portal/sample')
    driver.implicitly_wait(2)
    driver.find_element(By.XPATH, '/html/body/div[2]/app-root/div/div/div/ng-component/div[2]/button[3]').click() # Filter by data collection button
    driver.find_element(By.XPATH, '/html/body/div[2]/app-root/div/div/div/ng-component/div[3]/div/data-collection-filter/div/div[2]/div/div[1]/label').click() # 1000 Genomes 30x on GRCh38 checkbox
    driver.find_element(By.XPATH, '/html/body/div[2]/app-root/div/div/div/ng-component/div[3]/div/data-collection-filter/div/div[2]/div/div[3]/label').click() # 1000 Genomes on GRCh38 checkbox
    driver.find_element(By.XPATH, '/html/body/div[2]/app-root/div/div/div/ng-component/div[3]/div/data-collection-filter/div/div[2]/div/div[5]/label').click() # 1000 Genomes phase 1 release checkbox
    driver.find_element(By.XPATH, '/html/body/div[2]/app-root/div/div/div/ng-component/div[3]/div/data-collection-filter/div/div[2]/div/div[4]/label').click() # 1000 Genomes phase 3 release
    driver.find_element(By.XPATH, '/html/body/div[2]/app-root/div/div/div/ng-component/div[2]/button[4]').click() # Download the list button

    while os.path.isfile(f"{DOWNLOADS}/igsr_samples.tsv") == False:
        print("Looking for sample file")

    print("Found sample file")
    moved_file = os.getcwd() + '/igsr_samples.tsv'
    shutil.move(f"{DOWNLOADS}/igsr_samples.tsv", moved_file)
    
    subject_df = pd.read_csv('igsr_samples.tsv', sep='\t')
    print(subject_df.head(10))
    print("Converting subject df to fhirized json")
    subject_json_strings = []
    for index, row in subject_df.iterrows():
        subject_json_strings.append(convert_to_fhir_subject(row))

    subject_json_dict_list = [json.loads(json_str) for json_str in subject_json_strings]
    with open('1kgenomes_subject_fhirized.json', 'w') as f:
        json.dump(subject_json_dict_list, f, indent=4)
    print("Conversion of subject complete, see output dir for subject_fhirized.json")

if __name__ == "__main__":
    main()