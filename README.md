# FHIR-ETL
This repository's goal is to extract data from sources found by the NCPI FHIR Aggregator Team, convert the data therein to the FHIR standard, and make this FHIRized data available to the wider public.

Current FHIRized projects: 
1. **1000 Genomes** 
2. **GTeX**

More information about each can be found in associated folder's README.


## Usage 

### Installation

- from source 
```commandline
# clone repo & setup virtual env
python3 -m venv venv
. venv/bin/activate
pip install -e .
```

### Transform to FHIR 

#### 1kGenomes
```commandline
python 1kgenomes/1000g_fhirizer.py
python 1kgenomes/document_references.py
```

### Validate generated FHIR data

```commandline
python cli.py validate --path 1kgenomes/META

{'summary': {'DocumentReference': 48, 'Specimen': 3500, 'ResearchStudy': 1, 'ResearchSubject': 3500, 'Group': 1, 'Patient': 3500}}
```
