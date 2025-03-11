from fhir.resources.identifier import Identifier
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.codeablereference import CodeableReference
from fhir.resources.extension import Extension
from fhir.resources.patient import Patient
from fhir.resources.specimen import Specimen, SpecimenCollection, SpecimenContainer
from fhir.resources.researchstudy import ResearchStudy
from fhir.resources.researchsubject import ResearchSubject
from fhir.resources.fhirtypes import ReferenceType, QuantityType
from uuid import uuid3, uuid5, NAMESPACE_DNS
import uuid
import pandas as pd
import json

THOUSAND_GENOMES = 'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/'

class IDHelper: # pilfered from https://github.com/FHIR-Aggregator/CDA2FHIR/blob/7660b8ee9a7b815855a826bfb78aee62eb39cf27/cda2fhir/transformer.py#L34
    def __init__(self):
        self.project_id = '1KG'
        self.namespace = uuid3(NAMESPACE_DNS, THOUSAND_GENOMES)

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
        "id": IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{THOUSAND_GENOMES}", "technical/working/20130606_sample_info/"]), "value": str(input_row['Sample'])}), "Patient"),
        "identifier": [{"use":"official", "system": "https://gtexportal.org/home/downloads/adult-gtex/metadata", "value": input_row['Sample']}],
        "meta":{
            "profile": [
                "https://nih-ncpi.github.io/ncpi-fhir-ig-2/StructureDefinition-ncpi-participant.html"
            ]
        }}
    )

    extensions = []
    birth_sex = input_row['Gender']
    if pd.notna(birth_sex):
        extensions.append(Extension(**{
            "url": "https://hl7.org/fhir/us/core/STU3.1.1/StructureDefinition-us-core-sex.html", "valueString": birth_sex})
        )

    race = input_row['Population Description']
    if pd.notna(race):
        extensions.append(Extension(**{
            "url": "https://hl7.org/fhir/us/core/STU3.1.1/StructureDefinition-us-core-race.html", "valueString": race})
        )

    population = input_row['Population']
    if pd.notna(population):
        extensions.append(Extension(**{
            "url": "https://nih-ncpi.github.io/ncpi-fhir-ig-2/StructureDefinition-research-population.html", "valueString": population})
        )

    extensions.append(Extension(**{
        "url": "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study", 
        "valueReference": 
        {"reference": IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{THOUSAND_GENOMES}", "technical/working/20130606_sample_info/"]), "value": "1KG"}), "ResearchStudy")}}))

    if extensions:
        ncpi_participant.extension = extensions

    return json.dumps(ncpi_participant.dict(), indent=4)

def convert_to_fhir_researchsubject(input_row):
    IDMakerInstance = IDHelper()
    ncpi_studyparticipant = ResearchSubject(**{
        "resourceType": "ResearchSubject",
        "id": IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{THOUSAND_GENOMES}", "technical/working/20130606_sample_info/"]), "value": str(input_row['Sample'])}), "ResearchSubject"),
        "identifier": [{"use":"official", "system": "https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/20130606_sample_info/", "value": input_row['Sample']}],
        "subject": {
            "reference": "Patient/" + str(IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{THOUSAND_GENOMES}", "technical/working/20130606_sample_info/"]), "value": str(input_row['Sample'])}), "Patient"))
        },
        "status": "on-study",
        "study": {
            "reference": "ResearchStudy/" + str(IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{THOUSAND_GENOMES}", "technical/working/20130606_sample_info/"]), "value": "1KG"}), "ResearchStudy"))
        }
    })

    extensions = []
    extensions.append(Extension(**{
        "url": "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study", 
        "valueReference": {
            "reference": "ResearchStudy/" + IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{THOUSAND_GENOMES}", "technical/working/20130606_sample_info/"]), "value": "1KG"}), "ResearchStudy")
            }
        })
    )
    ncpi_studyparticipant.extension = extensions

    return json.dumps(ncpi_studyparticipant.dict(), indent = 4)

def convert_to_fhir_specimen(input_row):
    IDMakerInstance = IDHelper()
    sequencing_center_dict = {'454MSC': '454 Rocher', 'ABI': 'ABI Life Sciences', 'BCM': 'Baylor College of Medicine', 'BGI': 'Beijing Genome Institute', 'BI': 'The Broad Institute', 'ILLUMINA': 'Illumina',
    'MPIMG': 'Max Planck Institute for Molecular Genetics', 'SC': 'The Sanger Instute', 'WUGSC': 'Washington University Genome Sequencing Center', '': 'Not provided'}

    # problem: do not *want* to use a Reference style of reprsentation for bodySite at this juncture (lack of familiartiy with HL7 bodySite codes mostly and don't feel like learning, 
    # also am not sure GTeX's representation of bodySites would corrspond neatly to HL7's codes either). 
    # Want to use Concept style of presentation, but latest version of fhir.resources requires bodySite be of a CodeableReference type
    # see https://github.com/nazrulworld/fhir.resources/blob/main/fhir/resources/specimen.py#L296
    # solution: force a concept kind of presentation for a CodeableReference using the above and below.

    ncpi_sample = Specimen(**{
        "resourceType": "Specimen",
        "id": IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{THOUSAND_GENOMES}", "technical/working/20130606_sample_info/"]), "value": str(input_row['Sample'])}), "Specimen"),
        "identifier": [{"use": "official", "system": "https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/20130606_sample_info/", "value": input_row['Sample']}],
        "meta":{
            "profile": [
                "https://nih-ncpi.github.io/ncpi-fhir-ig-2/StructureDefinition-ncpi-sample.html"
            ]
        },
        "type": { # why can't I just do method and bodySite like type? why god? I'm going to find a way. Note that it's a codeableConcept type like method: 
            # https://github.com/nazrulworld/fhir.resources/blob/main/fhir/resources/specimen.py#L241
            "coding": [
                    {
                    "system": "https://terminology.hl7.org/CodeSystem-v3-SpecimenType.html",
                    "code": input_row['DNA Source from Coriell'] if pd.notna(input_row['DNA Source from Coriell']) else "Whole blood",
                    "display": "Lymphoblastoid Cell Line" if input_row['DNA Source from Coriell'] == 'LCL' else "Whole blood",
                    }
                ]
            },
        "subject": {"reference": IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{THOUSAND_GENOMES}", "technical/working/20130606_sample_info/"]), "value": str(input_row['Sample'])}), "Patient")} if pd.notna(input_row['Sample']) else "Not specified",
        "collection": SpecimenCollection(**{         
            "method": CodeableConcept(**{
                "coding": [
                    {
                    "system": "https://terminology.hl7.org/CodeSystem-v2-0488.html",
                    "code": input_row['Main project LC platform'] if pd.notna(input_row['Main project LC platform']) else 'Not specified',
                    "display": input_row['Main project LC platform'] if pd.notna(input_row['Main project LC platform']) else 'Not specified'
                    }
                ]}) # had to remove bodySite for compliance with the R4B validator in https://github.com/FHIR-Aggregator/submission/blob/main/fhir_aggregator_submission/prep.py#L115. Either fix how bodySite is coded to satisfy R5 *and* R4B or suggest a change to prep.py at some later point.
            #"bodySite": CodeableReference(
            #    concept = CodeableConcept(**{
            #    "coding": [
            #        {
            #            "system": "https://terminology.hl7.org/CodeSystem-v2-0163.html",
            #            "code": "Blood",
            #            "display": "Whole blood"
            #        }
            #    ]})
            })
        # possibly fix the below element later
        #"container": SpecimenContainer(**{
        #    "device": ReferenceType(**{
        #        "reference": "Device/not-provided-by-1KG"
        #        }),
        #    "location": ReferenceType(**{
        #        "reference": sequencing_center_dict[input_row["Main project LC Centers"]]
        #        }),
        #    "specimenQuantity": QuantityType(**{
        #        "value": input_row['Total LC Sequence'],
        #        "unit": "Low coverage whole genome sequencing count"
        #        })
        #})
    })

    extensions = []
    extensions.append(Extension(**{
        "url": "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study", 
        "valueReference": {
            "reference": IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{THOUSAND_GENOMES}", "technical/working/20130606_sample_info/"]), "value": "1KG"}), "ResearchStudy")
            }
        })
    )

    ncpi_sample.extension = extensions
    return json.dumps(ncpi_sample.dict(), indent = 4)

def main():        
    sample_df = pd.read_csv('https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/20130606_sample_info/20130606_sample_info.txt', sep='\t') 
    # sample_df.to_csv('20130606_sample_info.csv', index=False)

    IDMakerInstance = IDHelper()
    ncpi_researchstudy = ResearchStudy(
        **{
            "id": IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{THOUSAND_GENOMES}", "technical/working/20130606_sample_info/"]), "value": "1KG"}), "ResearchStudy"),
            "identifier": [Identifier(**{"system": "".join([f"https://{THOUSAND_GENOMES}", "technical/working/20130606_sample_info/"]), "value": "1KG"})],
            "title": "1000 Genomes Project Sample Metadata",
            "status": "active"
        }
    )
    extensions = []
    extensions.append(Extension(**{
        "url": "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study", 
        "valueReference": {
            "reference": "ResearchStudy/" + IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{THOUSAND_GENOMES}", "technical/working/20130606_sample_info/"]), "value": "1KG"}), "ResearchStudy")
            }
        })
    )
    ncpi_researchstudy.extension = extensions

    print(sample_df.head(10))
    print("Converting sample df to fhirized json")
    subject_json_strings = []
    researchsubject_json_strings =[]
    sample_json_strings = []
    for index, row in sample_df.iterrows():
        subject_json_strings.append(convert_to_fhir_subject(row))
        researchsubject_json_strings.append(convert_to_fhir_researchsubject(row))
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