import click
import os
import sys
import json
from pathlib import Path
import importlib.resources

@click.group()
def cli():
    """CLI for validating NDJSON files."""
    pass

@cli.command('validate')
@click.option("-d", "--debug", is_flag=True, default=False,
              help="Run in debug mode.")
@click.option("-p", "--path", default=None,
              help="Path to read the FHIR NDJSON files. Default is CDA2FHIR/data/META.")
def validate(debug: bool, path):
    """Validate the output FHIR NDJSON files."""
    from gen3_tracker.git import run_command  # Ensure gen3_tracker is installed
    from gen3_tracker.meta.validator import validate as validate_dir
    from halo import Halo
    INFO_COLOR = "green"
    ERROR_COLOR = "red"

    if not path:
        path = str(Path(importlib.resources.files('cda2fhir').parent / 'data' / 'META'))
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

if __name__ == "__main__":
    cli()

