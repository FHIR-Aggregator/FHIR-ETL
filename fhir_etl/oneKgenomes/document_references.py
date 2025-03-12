import orjson
import ftplib
import pandas as pd
import json
import requests
from datetime import datetime
from fhir_etl import utils

from fhir.resources.extension import Extension
from fhir.resources.group import Group
from fhir.resources.documentreference import DocumentReference
from fhir.resources.identifier import Identifier
from fhir.resources.reference import Reference

from pathlib import Path
import importlib.resources
import importlib

import mimetypes
mimetypes.add_type('text/vcf', '.vcf')

# -------------------------
# global
# -------------------------
IDMakerInstance = utils.IDHelper()

# -------------------------
# create DocumentReference from VCFs
# -------------------------

def create_document_reference(file_row):
    ftp_server = "ftp.1000genomes.ebi.ac.uk"
    ftp_directory = "/vol1/ftp/release/20130502/supporting/vcf_with_sample_level_annotation/"
    base_url = "https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/supporting/vcf_with_sample_level_annotation"

    file_name = file_row['file']
    file_size = file_row['size']
    mod_date = file_row['last_modified']
    # file_url = base_url + "/" + file_name
    data_format = utils.get_data_format(file_name)
    chromosome = utils.get_chromosome(file_name)

    # category entry if a chromosome is found.
    category = []
    if chromosome:
        category.append({
            "coding": [
                {
                    "system": "https://ftp.1000genomes.ebi.ac.uk/chromosome",
                    "code": chromosome,
                    "display": f"Chromosome {chromosome}"
                }
            ]
        })

    # attachment dictionary, file_size can't be  zero
    attachment = {
        "contentType": utils.get_mime_type(file_name),
        "url": base_url,
        "title": "file:///" + file_name
    }
    if file_size > 0:
        attachment["size"] = file_size

    extensions = []
    extensions.append(Extension(**{
        "url": "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study",
        "valueReference": {
            "reference": f"ResearchStudy/{IDMakerInstance.mint_id(Identifier(**{'system': ''.join([f'https://{utils.THOUSAND_GENOMES}', 'technical/working/20130606_sample_info/']), 'value': '1KG'}), 'ResearchStudy')}"
        }
    }))
    doc_ref_identifier = Identifier(
        **{"system": ftp_directory,
           "value": file_name})
    doc_ref_id = IDMakerInstance.mint_id(doc_ref_identifier, "DocumentReference")

    return DocumentReference(**{
        "id": doc_ref_id,
        "identifier": [
            {
                "use": "official",
                "system": base_url,
                "value": file_name
            }
        ],
        "version": "1",
        "status": "current",
        "type": {
            "coding": [
                {
                    "system": "https://ftp.1000genomes.ebi.ac.uk/data_format",
                    "code": data_format,
                    "display": data_format
                }
            ]
        },
        **({"category": category} if category else {}),
        "date": mod_date + "+00:00",
        "content": [
            {
                "attachment": attachment,
                "profile": [
                    {
                        "valueCoding": {
                            "system": "https://ftp.1000genomes.ebi.ac.uk/data_format",
                            "code": data_format,
                            "display": data_format
                        }
                    }
                ]
            }
        ],
        "extension": extensions
    })


def transform_1k_files():
    specimen_file = str(Path(importlib.resources.files('fhir_etl').parent / '1kgenomes' / 'META' / 'Specimen.ndjson'))
    assert specimen_file, "don't have Specimen.ndjson to derive file subject from..."

    ftp_server = "ftp.1000genomes.ebi.ac.uk"
    ftp_directory = "/vol1/ftp/release/20130502/supporting/vcf_with_sample_level_annotation/"
    base_url = "https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/supporting/vcf_with_sample_level_annotation"

    ftp = ftplib.FTP(ftp_server)
    ftp.login()  # Anonymous login
    ftp.cwd(ftp_directory)
    files = ftp.nlst()

    file_info = []
    for file in files:
        # only files that contain 'vcf' (e.g., vcf or vcf.gz)
        if "vcf" not in file.lower():
            continue

        # file size; default to 0 if unavailable
        try:
            size = ftp.size(file)
            if size is None:
                size = 0
        except Exception:
            size = 0

        # last modified date using MDTM command
        try:
            mdtm_response = ftp.sendcmd("MDTM " + file)
            last_modified = utils.parse_mdtm(mdtm_response)
        except Exception:
            last_modified = datetime.now().isoformat()

        file_info.append({'file': file, 'size': size, 'last_modified': last_modified})

    ftp.quit()

    df_release = pd.DataFrame(file_info)
    df_release = df_release.dropna(subset=["file"])

    doc_refs = [create_document_reference(row) for _, row in df_release.iterrows()]
    # -------------------------
    # extract Sample IDs from VCF Header
    # -------------------------
    header_url = "https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/supporting/vcf_with_sample_level_annotation/header"
    response = requests.get(header_url)
    response.raise_for_status()
    header_text = response.text

    vcf_header_line = None
    for line in header_text.splitlines():
        if line.startswith("#CHROM"):
            vcf_header_line = line
            break

    if not vcf_header_line:
        raise Exception("Could not find the '#CHROM' header line in the header file.")

    columns = vcf_header_line.strip().split("\t")
    if len(columns) <= 9:
        raise Exception("Expected sample IDs after the first 9 columns, but found none.")

    sample_ids_from_header = columns[9:]
    print(f"Extracted {len(sample_ids_from_header)} sample IDs from header:")
    print(sample_ids_from_header)

    # -------------------------
    # compare with Specimen.ndjson
    # -------------------------
    specimen_system = "https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/20130606_sample_info/"

    specimen_sample_ids = set()
    with open(specimen_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                specimen = json.loads(line)
            except json.JSONDecodeError:
                print("Skipping invalid JSON line.")
                continue
            if "identifier" in specimen:
                for identifier in specimen["identifier"]:
                    if identifier.get("system") == specimen_system:
                        value = identifier.get("value")
                        if value:
                            specimen_sample_ids.add(value)

    print(f"Found {len(specimen_sample_ids)} sample IDs in Specimen.ndjson.")

    header_sample_ids = set(sample_ids_from_header)
    found_ids = header_sample_ids.intersection(specimen_sample_ids)
    missing_ids = header_sample_ids.difference(specimen_sample_ids)

    print("\nSummary of Sample IDs generated and found in VCF header:")
    print(f"Total sample IDs from header: {len(header_sample_ids)}")
    print(f"Sample IDs found in Specimen.ndjson: {len(found_ids)}")
    print(f"Sample IDs missing in Specimen.ndjson: {len(missing_ids)}")

    specimen_ids = ["Specimen/" + IDMakerInstance.mint_id(Identifier(**{"system": "".join([f"https://{utils.THOUSAND_GENOMES}", "technical/working/20130606_sample_info/"]), "value": str(_id)}), "Specimen") for _id in found_ids]

    group_identifier = Identifier(**{"system": "".join([f"https://{utils.THOUSAND_GENOMES}", "technical/working/20130606_sample_info/"]), "value": header_url})
    group_id = IDMakerInstance.mint_id(group_identifier, "Group")

    group_resource = Group(**{
        "id": group_id,
        "identifier": [group_identifier],
        "membership": "definitional",
        "type": "specimen",
        "member": [{"entity": {"reference": sid}} for sid in specimen_ids]
        })

    for doc_ref in doc_refs:
        doc_ref.subject = Reference(**{"reference": f"Group/{group_resource.id}"})

    # -------------------------
    # output to ndjson files
    # -------------------------
    folder_path = str(Path(importlib.resources.files('fhir_etl').parent / 'fhir_etl' /'onekgenomes' / 'META' ))

    document_references = {_doc_ref.id: _doc_ref for _doc_ref in doc_refs if _doc_ref}.values()
    fhir_document_references = [orjson.loads(doc_ref.json()) for doc_ref in document_references]
    cleaned_fhir_document_references = utils.clean_resources(fhir_document_references)
    utils.create_or_extend(new_items=cleaned_fhir_document_references, folder_path=folder_path,
                           resource_type='DocumentReference', update_existing=False)

    fhir_group = [orjson.loads(group.json()) for group in [group_resource]]
    cleaned_fhir_groups = utils.clean_resources(fhir_group)
    utils.create_or_extend(new_items=cleaned_fhir_groups, folder_path=folder_path,
                           resource_type='Group', update_existing=False)

