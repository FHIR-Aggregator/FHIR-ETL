import os
import orjson
from fhir.resources import get_fhir_model_class
import ftplib
import pandas as pd
import json
import re
import requests
from datetime import datetime

from fhir.resources.extension import Extension
from fhir.resources.group import Group
from fhir.resources.documentreference import DocumentReference
from fhir.resources.identifier import Identifier
from fhir.resources.reference import Reference
from fhir.resources.fhirresourcemodel import FHIRAbstractModel

import uuid
from uuid import uuid3, uuid5, NAMESPACE_DNS

import decimal
import importlib

import mimetypes
mimetypes.add_type('text/vcf', '.vcf')

THOUSAND_GENOMES = 'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/'

class IDHelper:  # pilfered from https://github.com/FHIR-Aggregator/CDA2FHIR/blob/7660b8ee9a7b815855a826bfb78aee62eb39cf27/cda2fhir/transformer.py#L34
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


def get_data_format(file_name):
    """
    Derive the data format from the file name by removing known compression/index extensions
    Returns the remaining extension in uppercase, or 'UNKNOWN' if not found
    """
    parts = file_name.split('.')
    ignore = {"gz", "tbi", "csi"}
    # Remove trailing parts that are in the ignore list.
    while len(parts) > 1 and parts[-1].lower() in ignore:
        parts.pop()
    return parts[-1].upper() if len(parts) > 1 else "UNKNOWN"


def get_chromosome(file_name):
    """
    Search for a chromosome pattern (e.g. chr1, chrX, chrMT) in the file name
    Returns the chromosome (without the "chr" prefix, in uppercase) if found; otherwise, None
    """
    match = re.search(r'\bchr([0-9XYMT]+)\b', file_name, re.IGNORECASE)
    return match.group(1).upper() if match else None


def parse_mdtm(mdtm_str):
    """
    Given an MDTM response string like '213 20220509124500', return an ISO datetime string
    """
    try:
        mod_time = datetime.strptime(mdtm_str[4:], "%Y%m%d%H%M%S")
        return mod_time.isoformat()
    except Exception:
        return datetime.now().isoformat()

def get_mime_type(file_name):
    """Get mime type from file name."""
    return mimetypes.guess_type(file_name, strict=False)[0] or 'application/octet-stream'

def is_valid_fhir_resource_type(resource_type):
    try:
        model_class = get_fhir_model_class(resource_type)
        return model_class is not None
    except KeyError:
        return False

def create_or_extend(new_items, folder_path='META', resource_type='Observation', update_existing=False):
    assert is_valid_fhir_resource_type(resource_type), f"Invalid resource type: {resource_type}"

    file_name = "".join([resource_type, ".ndjson"])
    file_path = os.path.join(folder_path, file_name)

    file_existed = os.path.exists(file_path)

    existing_data = {}

    if file_existed:
        with open(file_path, 'r') as file:
            for line in file:
                try:
                    item = orjson.loads(line)
                    existing_data[item.get("id")] = item
                except orjson.JSONDecodeError:
                    continue

    for new_item in new_items:
        new_item_id = new_item["id"]
        if new_item_id not in existing_data or update_existing:
            existing_data[new_item_id] = new_item

    with open(file_path, 'w') as file:
        for item in existing_data.values():
            file.write(orjson.dumps(item).decode('utf-8') + '\n')

    if file_existed:
        if update_existing:
            print(f"{file_name} has new updates to existing data.")
        else:
            print(f"{file_name} has been extended, without updating existing data.")
    else:
        print(f"{file_name} has been created.")


def remove_empty_dicts(data):
    """
    Recursively remove empty dictionaries and lists from nested data structures.
    """
    if isinstance(data, dict):
        new_data = {}
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                cleaned = remove_empty_dicts(v)
                # keep non-empty structures or zero
                if cleaned or cleaned == 0:
                    new_data[k] = cleaned
            # keep values that are not empty or zero
            elif v or v == 0:
                new_data[k] = v
        return new_data

    elif isinstance(data, list):
        cleaned_list = [remove_empty_dicts(item) for item in data]
        cleaned_list = [item for item in cleaned_list if item or item == 0]  # remove empty items
        return cleaned_list if cleaned_list else None  # return none if list is empty

    else:
        return data


def validate_fhir_resource_from_type(resource_type: str, resource_data: dict) -> FHIRAbstractModel:
    """
    Generalized function to validate any FHIR resource type using its name.
    """
    try:
        resource_module = importlib.import_module(f"fhir.resources.{resource_type.lower()}")
        resource_class = getattr(resource_module, resource_type)
        return resource_class.model_validate(resource_data)

    except (ImportError, AttributeError) as e:
        raise ValueError(f"Invalid resource type: {resource_type}. Error: {str(e)}")


def convert_decimal_to_float(data):
    """Convert pydantic Decimal to float"""
    if isinstance(data, dict):
        return {k: convert_decimal_to_float(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_decimal_to_float(item) for item in data]
    elif isinstance(data, decimal.Decimal):
        return float(data)
    else:
        return data


def convert_value_to_float(data):
    """
    Recursively converts all general 'entity' -> 'value' fields in a nested dictionary or list
    from strings to float or int.
    """
    if isinstance(data, list):
        return [convert_value_to_float(item) for item in data]
    elif isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict) and 'value' in value:
                if isinstance(value['value'], str):
                    if value['value'].replace('.', '').replace('-', '', 1).isdigit() and "." in value['value']:
                        value['value'] = float(value['value'])
                    elif value['value'].replace('.', '').replace('-', '', 1).isdigit() and "." not in value['value']:
                        value['value'] = int(value['value'])
            else:
                data[key] = convert_value_to_float(value)
    return data


def clean_resources(entities):
    cleaned_resource = []
    for resource in entities:
        if hasattr(resource, "dict"):
            resource_dict = resource.dict()
        else:
            resource_dict = resource

        resource_type = resource_dict["resourceType"]
        cleaned_resource_dict = remove_empty_dicts(resource_dict)
        try:
            validated_resource = validate_fhir_resource_from_type(resource_type, cleaned_resource_dict).model_dump_json()
        except ValueError as e:
            print(f"Validation failed for {resource_type}: {e}")
            continue

        # Handle pydantic Decimal cases
        validated_resource = convert_decimal_to_float(orjson.loads(validated_resource))
        validated_resource = convert_value_to_float(validated_resource)
        validated_resource = orjson.loads(orjson.dumps(validated_resource).decode("utf-8"))
        cleaned_resource.append(validated_resource)

    return cleaned_resource

