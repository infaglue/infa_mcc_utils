# ----------------------------------------------------------------------------------------------------------------------------------------------
# Script to import classifications into Informatica Cloud Data Governance and Catalog (CDGC)
# ----------------------------------------------------------------------------------------------------------------------------------------------
import argparse
import sys
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import logging
from typing import Dict, Any, List, Optional
from idmc_wrapper import IDMCAuth, IDMCAuthenticationError, log_usage
from cdgc_internal_wrapper import CDGCInternal

# ----------------------------------------------------------------------------------------------------------------------------------------------
# Parameter setup
# ----------------------------------------------------------------------------------------------------------------------------------------------
load_dotenv()
username = os.getenv("INFORMATICA_USERNAME")
password = os.getenv("INFORMATICA_PASSWORD")
login_url = os.getenv("INFORMATICA_LOGIN_URL", "https://dm-us.informaticacloud.com")
pod_api_url = os.getenv("INFORMATICA_POD_API_URL", "https://usw1.dmp-us.informaticacloud.com")
cdgc_api_url = os.getenv("INFORMATICA_CDGC_API_URL", "https://cdgc-api.dm-us.informaticacloud.com")


# ----------------------------------------------------------------------------------------------------------------------------------------------
# Classes and Functions
# ----------------------------------------------------------------------------------------------------------------------------------------------
class HelpOnErrorParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_help()
        print(f'\nError: {message}', file=sys.stderr)
        sys.exit(2)


def read_classification_file(filepath: str) -> Dict[str, Any]:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        logging.info(f"Successfully read file : {filepath}")

        if 'name' not in data:
            raise ValueError("Invalid classification file: missing 'name' field")

        return data

    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in file {filepath}: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to load file {filepath}: {e}")
        raise


def prepare_classification_for_import(classification: Dict[str, Any]) -> Dict[str, Any]:
    prepared = classification.copy()

    metadata_fields = ['export_date', 'export_org', 'export_user', 'id']
    for field in metadata_fields:
        if field in prepared:
            del prepared[field]

    return prepared


def classification_exists(client: CDGCInternal, classification_name: str) -> Optional[Dict[str, Any]]:
    try:
        classifications = client.classifications.get_all_classifications()

        for c in classifications:
            if c.get('name', '').lower() == classification_name.lower():
                return c

        return None

    except Exception as e:
        logging.error(f"Error checking for existing classification: {e}")
        raise


def import_classification(client: CDGCInternal, classification: Dict[str, Any], update_if_exists: bool = False) -> Dict[str, Any]:
    classification_name = classification.get('name', 'Unknown')
    logging.info(f"Processing classification: {classification_name}")

    try:
        prepared = prepare_classification_for_import(classification)

        existing = classification_exists(client, classification_name)

        if existing:
            if update_if_exists:
                logging.info(f"Updating existing classification: {classification_name}")
                response = client.classifications.update_classification(existing.get('id'), prepared)
                logging.info(f"Successfully updated: {classification_name}")
            else:
                logging.info(f"Skipped (already exists): {classification_name}")
        else:
            logging.info(f"Creating new classification: {classification_name}")
            response = client.classifications.import_classification(prepared)
            logging.info(f"Successfully created: {classification_name}")

    except Exception as e:
        logging.error(f"Failed to import {classification_name}: {e}")
        logging.error(f"Exception details: {type(e).__name__}: {str(e)}", exc_info=True)


def import_classification_from_file(client: CDGCInternal, filepath: str, update_if_exists: bool = False) -> Dict[str, Any]:
    classification = read_classification_file(filepath)

    import_classification(client, classification, update_if_exists)


def import_all_from_directory(client: CDGCInternal, directory: str, update_if_exists: bool = False) -> List[Dict[str, Any]]:
    if not os.path.isdir(directory):
        raise NotADirectoryError(f"Not a directory: {directory}")

    json_files = [f for f in os.listdir(directory) if f.endswith('.json')]

    if not json_files:
        logging.warning(f"No JSON files found in directory: {directory}")
        return []

    total = len(json_files)
    logging.info(f"Found {total} JSON files to process")

    for idx, filename in enumerate(json_files, 1):
        filepath = os.path.join(directory, filename)
        logging.info(f"[{idx}/{total}] Processing: {filename}")

        try:
            import_classification_from_file(client, filepath, update_if_exists)

        except Exception as e:
            logging.error(f"Failed to process {filename}: {e}")


def main():

    parser = HelpOnErrorParser(
        description="Import IDMC CDGC Classifications from JSON files",
        formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=55, width=125)
    )

    parser.add_argument('-f', '--file', help='Path to classification JSON file to import')
    parser.add_argument('-d', '--directory', help='Directory containing classification JSON files to import')
    parser.add_argument('-u', '--update', action='store_true', help='Update classification if it already exists (default: skip)')

    args = parser.parse_args()

    log_level = logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)-5s - %(message)s', force=True)

    logging.info("Starting Classification Import")
    logging.info("Parameters:")
    logging.info(f"\t- File: {args.file}")
    logging.info(f"\t- Directory: {args.directory}")
    logging.info(f"\t- Update: {args.update}")

    if not args.file and not args.directory:
        logging.error("Error: Must specify either --file or --directory")
        parser.print_help()
        sys.exit(1)

    if args.file and args.directory:
        logging.error("Error: Cannot specify both --file and --directory")
        sys.exit(1)

    try:
        logging.info(f"Authenticating user: {username}...")
        auth = IDMCAuth(username=username, password=password, login_url=login_url)
        auth.login()
        auth.generate_jwt_token()
        logging.info(f"Authentication successful")

        client = CDGCInternal(auth=auth, cdgc_api_url=cdgc_api_url)

        if args.file:
            import_classification_from_file(client, args.file, args.update)
        elif args.directory:
            import_all_from_directory(client, args.directory, args.update)

        logging.info("Import process completed!")

        log_usage(auth.org_name, os.path.basename(__file__), "Classification Import")

    except IDMCAuthenticationError as e:
        logging.error(f"Authentication Error: {e}")
        logging.error("Please check your credentials and login URL.")
        logging.error("Authentication error details:", exc_info=True)
        sys.exit(1)

    except FileNotFoundError as e:
        logging.error(f"File Error: {e}")
        sys.exit(1)

    except Exception as e:
        logging.error(f"Unexpected Error: {e}")
        logging.error("Unexpected error details:", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()