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
fhir_etl transform -p 1kgenomes
fhir_etl transform -p gtex
```

### Validate generated FHIR data

```commandline
fhir_etl validate --path fhir_etl/oneKgenomes/META
{'summary': {'DocumentReference': 48, 'Specimen': 3500, 'ResearchStudy': 1, 'ResearchSubject': 3500, 'Group': 1, 'Patient': 3500}}

fhir_etl validate --path fhir_etl/GTeX/META
{'summary': {'DocumentReference': 49, 'Specimen': 43559, 'ResearchStudy': 1, 'ResearchSubject': 980, 'Group': 1, 'Patient': 980}}
```
