import click
import os
import sys
import json
from pathlib import Path
import importlib.resources
from fhir_etl.oneKgenomes.oneKg_fhirizer import transform_1k
from fhir_etl.oneKgenomes.document_references import transform_1k_files
from fhir_etl.GTeX.gtex_fhirizer import transform_gtex


@click.group()
def cli():
    """CLI for validating NDJSON files."""
    pass

@cli.command('validate')
@click.option("-d", "--debug", is_flag=True, default=False,
              help="Run in debug mode.")
@click.option("-p", "--path", default=None,
              help="Path to read the FHIR NDJSON files.")
def validate(debug: bool, path):
    """Validate the output FHIR NDJSON files."""
    from gen3_tracker.git import run_command  # Ensure gen3_tracker is installed
    from gen3_tracker.meta.validator import validate as validate_dir
    from halo import Halo
    INFO_COLOR = "green"
    ERROR_COLOR = "red"

    if not os.path.isdir(path):
        raise ValueError(f"Path: '{path}' is not a valid directory.")

    try:
        with Halo(text='Validating', spinner='line', placement='right', color='white'):
            result = validate_dir(path)
        click.secho(result.resources, fg=INFO_COLOR, file=sys.stderr)
        for err in result.exceptions:
            click.secho(f"{err.path}:{err.offset} {err.exception} {json.dumps(err.json_obj, separators=(',', ':'))}",
                        fg=ERROR_COLOR, file=sys.stderr)
        if result.exceptions:
            sys.exit(1)
    except Exception as e:
        click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
        if debug:
            raise

@cli.command('transform')
@click.option("-p", "--project", default=None,
              help="Project name 1kgenomes or gtex.")
def transformer(project):
    assert project in ['1kgenomes', 'gtex']

    if project == "1kgenomes":
        meta_path = str(Path(importlib.resources.files('fhir_etl').parent / 'fhir_etl' /'onekgenomes' / 'META' ))
        if not os.path.isdir(meta_path):
            os.makedirs(meta_path, exist_ok=True)
        transform_1k()
        transform_1k_files()
    if project == "gtex":
        meta_path = str(Path(importlib.resources.files('fhir_etl').parent / 'fhir_etl' /'GTEx' / 'META' ))
        if not os.path.isdir(meta_path):
            os.makedirs(meta_path, exist_ok=True)
        transform_gtex()

if __name__ == "__main__":
    cli()

