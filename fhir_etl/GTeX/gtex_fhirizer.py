from fhir.resources.identifier import Identifier
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.codeablereference import CodeableReference
from fhir.resources.extension import Extension
from fhir.resources.patient import Patient
from fhir.resources.specimen import Specimen, SpecimenCollection
from fhir.resources.researchstudy import ResearchStudy
from fhir.resources.researchsubject import ResearchSubject
from uuid import uuid3, uuid5, NAMESPACE_DNS
import uuid
import pandas as pd
import json
import requests

GTEX_SITE = 'gtexportal.org/home/'

class IDHelper: # pilfered from https://github.com/FHIR-Aggregator/CDA2FHIR/blob/7660b8ee9a7b815855a826bfb78aee62eb39cf27/cda2fhir/transformer.py#L34
    def __init__(self):
        self.project_id = 'GTEX'
        self.namespace = uuid3(NAMESPACE_DNS, GTEX_SITE)

    @staticmethod
    def is_valid_uuid(value: str) -> bool:
        if value is None:
            return False
        try:
            _obj = uuid.UUID(value, version=5)
        except ValueError:
            return False
        return True

    def mint_id(self, identifier: Identifier | str, resource_type: str = None) -> str: 
        """create a UUID from an identifier. - mint id via Walsh's convention
        https://github.com/ACED-IDP/g3t_etl/blob/d095895b0cf594c2fd32b400e6f7b4f9384853e2/g3t_etl/__init__.py#L61"""
        # dispatch on type
        if isinstance(identifier, Identifier):
            assert resource_type, "resource_type is required for Identifier"
            identifier = f"{resource_type}/{identifier.system}|{identifier.value}"
        return self._mint_id(identifier)
    
    def _mint_id(self, identifier_string: str) -> str:
        """create a UUID from an identifier, insert project_id."""
        return str(uuid5(self.namespace, f"{self.project_id}/{identifier_string}"))

def retrieve_paginated_gtex_data(api_endpoint):
    page = 0
    all_data = []
    try: # there is a chance that GTEx's API is down for a particular parameter set. If this happens, coming back the next day *usually* solves the problem. Or adjust 'datasetId' on both line 49 and 60 to gtex_v8
        response = requests.get(api_endpoint, params={'datasetId': 'gtex_v10', 'itemsPerPage': 100, 'page': page}).json()
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)
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

def output_to_ndjson(json_str_list, filename):
    if filename == 'ResearchStudy':
        with open(f'{filename}.ndjson', 'w') as f:
            json_string = json.dumps(json_str_list)
            f.write(json_string + "\n")
    else:
        with open(f'{filename}.ndjson', 'w') as f:
            for entry in json_str_list:
                json_string = json.dumps(entry)
                f.write(json_string + "\n")
    print(f"Conversion complete, see output dir for {filename}.ndjson")

def convert_to_fhir_subject(input_row):
    IDMakerInstance = IDHelper()
    ncpi_participant = Patient(**{
        "resourceType": "Patient",
        "id": IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{GTEX_SITE}", "downloads/adult-gtex/metadata"]), "value": str(input_row['subjectId'])}), "Patient"),
        "identifier": [{"use":"official", "system": "https://gtexportal.org/home/downloads/adult-gtex/metadata", "value": input_row['subjectId']}],
        "meta":{
            "profile": [
                "https://nih-ncpi.github.io/ncpi-fhir-ig-2/StructureDefinition-ncpi-participant.html"
            ]
        },
        "deceasedBoolean": pd.notna(input_row['hardyScale'])
        }
    )

    extensions = []
    birth_sex = input_row['sex']
    if pd.notna(birth_sex):
        extensions.append(Extension(**{
            "url": "https://hl7.org/fhir/us/core/STU3.1.1/StructureDefinition-us-core-sex.html", "valueString": birth_sex})
        )

    death_circumstance = input_row['hardyScale']

    if pd.isna(death_circumstance):
        extensions.append(Extension(**
            {"url": "https://hl7.org/fhir/extensions/SearchParameter-patient-extensions-Patient-age.html",
            "valueString": f"{2025 - int(input_row['ageBracket'].split('-')[1])} - {2025 - int(input_row['ageBracket'].split('-')[0])}" # age is displayed in the form of 60-69 in input phenotype data as an example. Final year estimate should look like 1964 - 1975.
            })
        )
    if pd.notna(death_circumstance):
        extensions.append(Extension(**
            {"url": "https://hl7.org/fhir/R4B/extension-condition-dueto.html", "valueString": death_circumstance})
        )

    extensions.append(Extension(**{
        "url": "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study", 
        "valueReference": 
        {"reference": "ResearchStudy/" + IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{GTEX_SITE}", "downloads/adult-gtex/metadata"]), "value": "GTEX_V10"}), "ResearchStudy")}})
        )

    if extensions:
        ncpi_participant.extension = extensions

    return json.dumps(ncpi_participant.dict(), indent = 4)

def convert_to_fhir_researchsubject(input_row):
    IDMakerInstance = IDHelper()
    ncpi_studyparticipant = ResearchSubject(**{
        "resourceType": "ResearchSubject",
        "id": IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{GTEX_SITE}", "downloads/adult-gtex/metadata"]), "value": str(input_row['subjectId'])}), "ResearchSubject"),
        "identifier": [{"use":"official", "system": "https://gtexportal.org/home/downloads/adult-gtex/metadata", "value": input_row['subjectId']}],
        "subject": {
            "reference": "Patient/" + str(IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{GTEX_SITE}", "downloads/adult-gtex/metadata"]), "value": str(input_row['subjectId'])}), "Patient"))
        },
        "status": "on-study",
        "study": {
            "reference": "ResearchStudy/" + str(IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{GTEX_SITE}", "downloads/adult-gtex/metadata"]), "value": "GTEX_V10"}), "ResearchStudy"))
        }}
    )

    extensions = []
    extensions.append(Extension(**{
        "url": "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study", 
        "valueReference": {
            "reference": "ResearchStudy/" + IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{GTEX_SITE}", "downloads/adult-gtex/metadata"]), "value": "GTEX_V10"}), "ResearchStudy")
            }
        })
    )
    ncpi_studyparticipant.extension = extensions

    return json.dumps(ncpi_studyparticipant.dict(), indent = 4)

def convert_to_fhir_specimen(input_row):
    IDMakerInstance = IDHelper()

    # problem: do not *want* to use a Reference style of reprsentation for bodySite at this juncture (lack of familiartiy with HL7 bodySite codes mostly and don't feel like learning, 
    # also am not sure GTeX's representation of bodySites would corrspond neatly to HL7's codes either). 
    # Want to use Concept style of presentation, but latest version of fhir.resources requires bodySite be of a CodeableReference type
    # see https://github.com/nazrulworld/fhir.resources/blob/main/fhir/resources/specimen.py#L296
    # solution: force a concept kind of presentation for a CodeableReference using the above and below.

    ncpi_sample = Specimen(**{
        "resourceType": "Specimen",
        "id": IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{GTEX_SITE}", "downloads/adult-gtex/metadata"]), "value": str(input_row['aliquotId'])}), "Specimen"),
        "identifier": [{"use": "official", "system": "https://gtexportal.org/home/downloads/adult-gtex/metadata", "value": input_row['aliquotId']}],
        "meta":{
            "profile": [
                "https://nih-ncpi.github.io/ncpi-fhir-ig-2/StructureDefinition-ncpi-sample.html"
            ]
        },
        "type": { # why can't I just do method and bodySite like I can do type right here? why god? I'm going to find a way. Note that it's a codeableConcept type like method: 
            # https://github.com/nazrulworld/fhir.resources/blob/main/fhir/resources/specimen.py#L241
            "coding": [
                    {
                    "system": "https://terminology.hl7.org/CodeSystem-v3-SpecimenType.html",
                    "code": input_row['dataType'] if pd.notna(input_row['dataType']) else 'None',
                    "display": input_row['dataType'] if pd.notna(input_row['dataType']) else 'None',
                    }
                ]
            },
        "subject": {"reference": "Patient/" + IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{GTEX_SITE}", "downloads/adult-gtex/metadata"]), "value": str(input_row['subjectId'])}), "Patient")} if pd.notna(input_row['subjectId']) else "Not specified",
        "collection": SpecimenCollection(**{         
            "method": CodeableConcept(**{
                "coding": [
                    {
                    "system": "https://terminology.hl7.org/CodeSystem-v2-0488.html",
                    "code": input_row['freezeType'],
                    "display": input_row['freezeType']
                    }
                ]})
            #"bodySite": CodeableReference(**{ # had to remove bodySite for compliance with the R4B validator in https://github.com/FHIR-Aggregator/submission/blob/main/fhir_aggregator_submission/prep.py#L115. Either fix how bodySite is coded to satisfy R5 *and* R4B or suggest a change to prep.py at some later point.
            #    "concept" : CodeableConcept(**{
            #    "coding": [
            #        {
            #            "system": "https://terminology.hl7.org/CodeSystem-v2-0163.html",
            #            "code": input_row['tissueSiteDetailId'],
            #            "display": input_row['tissueSiteDetail']
            #        }
            #    ]})
            #})
        })
    })

    extensions = []
    extensions.append(Extension(**{
        "url": "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study", 
        "valueReference": {
            "reference": "ResearchStudy/" + IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{GTEX_SITE}", "downloads/adult-gtex/metadata"]), "value": "GTEX_V10"}), "ResearchStudy")
            }
        })
    )

    ncpi_sample.extension = extensions
    return json.dumps(ncpi_sample.dict(), indent = 4)

def main():
    subject_endpoint = "https://gtexportal.org/api/v2/dataset/subject"
    sample_endpoint = "https://gtexportal.org/api/v2/dataset/sample"

    subject_data = retrieve_paginated_gtex_data(subject_endpoint)
    sample_data = retrieve_paginated_gtex_data(sample_endpoint)

    subject_df = pd.DataFrame(subject_data)
    #subject_df.to_csv('gtex_subject.csv', index = False)
    #subject_df = pd.read_csv('gtex_subject.csv')
    sample_df = pd.DataFrame(sample_data)
    #sample_df.to_csv('gtex_sample.csv', index = False)
    #sample_df = pd.read_csv('gtex_sample.csv')

    IDMakerInstance = IDHelper()
    ncpi_researchstudy = ResearchStudy(**{
            "id": IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{GTEX_SITE}", "downloads/adult-gtex/metadata"]), "value": "GTEX_V10"}), "ResearchStudy"),
            "identifier": [Identifier(**{"system": "".join([f"https://{GTEX_SITE}", "downloads/adult-gtex/metadata"]), "value": "GTEX_V10"})],
            "title": "GTEX Analysis v10 Adult Sample and Subject Metadata",
            "status": "active"
        }
    )
    extensions = []
    extensions.append(Extension(**{
        "url": "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study", 
        "valueReference": {
            "reference": "ResearchStudy/" + IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{GTEX_SITE}", "downloads/adult-gtex/metadata"]), "value": "GTEX_V10"}), "ResearchStudy")
            }
        })
    )
    ncpi_researchstudy.extension = extensions

    #print(ncpi_researchstudy)
    print("Subject dataframe:")
    print(subject_df.head(10))
    print("Converting subject df to fhirized json")
    subject_json_strings = []
    researchsubject_json_strings =[]
    for index, row in subject_df.iterrows():
        subject_json_strings.append(convert_to_fhir_subject(row))
        researchsubject_json_strings.append(convert_to_fhir_researchsubject(row))

    print("Sample dataframe")
    print(sample_df.head(10))
    print("Converting sample df to fhirized json")
    sample_json_strings = []
    for index, row in sample_df.iterrows():
        sample_json_strings.append(convert_to_fhir_specimen(row))

    subject_json_dict_list = [json.loads(json_str) for json_str in subject_json_strings]
    researchsubject_json_dict_list = [json.loads(json_str) for json_str in researchsubject_json_strings]
    sample_json_dict_list = [json.loads(json_str) for json_str in sample_json_strings]

    print("Converting subject_json_dict to Patient.ndjson")
    output_to_ndjson(subject_json_dict_list, 'Patient')
    print("Converting researchsubject_json_dict to ResearchSubject.ndjson")
    output_to_ndjson(researchsubject_json_dict_list, 'ResearchSubject')
    print("Converting sample_json_dict to Specimen.ndjson")
    output_to_ndjson(sample_json_dict_list, 'Specimen')
    print("Converting researchstudy_json_dict to ResearchStudy.ndjson")
    output_to_ndjson(ncpi_researchstudy.dict(), 'ResearchStudy')


if __name__ == "__main__":
    main()