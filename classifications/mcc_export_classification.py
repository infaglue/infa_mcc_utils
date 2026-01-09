# ----------------------------------------------------------------------------------------------------------------------------------------------
# Script to export classifications from Informatica Cloud Data Governance and Catalog (CDGC)
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
def create_json_filename(base_name: str) -> str:
    """
    Create a standardized JSON filename with timestamp

    Args:
        base_name: Base name for the file

    Returns:
        Formatted filename with timestamp
    """
    # Remove special characters and spaces
    safe_name = "".join(c if c.isalnum() or c in ('-', '_') else '_' for c in base_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{safe_name}_{timestamp}.json"


class ClassificationExporter:
    """Handles exporting classifications from IDMC CDGC"""

    def __init__(self, auth: IDMCAuth, cdgc_api_url: str):
        """
        Initialize the classification exporter

        Args:
            auth: IDMCAuth instance with valid authentication
            cdgc_api_url: CDGC API base URL (e.g., 'https://cdgc-api.dm-us.informaticacloud.com')
        """
        logging.debug("=" * 80)
        logging.debug("Initializing ClassificationExporter")
        self.auth = auth
        self.cdgc_api_url = cdgc_api_url

        # Initialize CDGC Internal client
        self.client = CDGCInternal(auth=auth, cdgc_api_url=cdgc_api_url)

        logging.debug(f"CDGC API URL: {self.cdgc_api_url}")
        logging.debug(f"Auth session ID exists: {bool(self.auth.session_id)}")
        logging.debug("=" * 80)


    def save_classification_to_file(self, classification: Dict[str, Any], filename: str, output_dir: str = "./output"):
        """
        Save classification data to JSON file with export metadata

        Args:
            classification: Classification data dictionary
            filename: Output filename
            output_dir: Output directory path
        """
        logging.debug(f"save_classification_to_file() called")
        logging.debug(f"Filename: {filename}")
        logging.debug(f"Output dir: {output_dir}")

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Add export metadata
        classification['export_date'] = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        classification['export_org'] = self.auth.org_name
        classification['export_user'] = self.auth.user_id

        # Full output path
        output_path = os.path.join(output_dir, filename)

        logging.debug(f"Full output path: {output_path}")

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(classification, f, indent=2, ensure_ascii=False)

            logging.info(f"Successfully saved classification to: {output_path}")
            logging.debug(f"File size: {os.path.getsize(output_path)} bytes")

        except Exception as e:
            logging.error(f"Failed to save file {output_path}: {e}")
            logging.debug(f"Exception details: {type(e).__name__}: {str(e)}", exc_info=True)
            raise


    def export_classification(self, classification_id: str, output_dir: str = "./output", filename: Optional[str] = None):
        """
        Export a single classification by ID

        Args:
            classification_id: Classification identifier
            output_dir: Output directory path
            filename: Custom filename (auto-generated if None)
        """
        logging.debug("=" * 80)
        logging.debug(f"export_classification() called")
        logging.debug(f"Classification ID: {classification_id}")

        # Fetch classification details
        logging.info("Fetching classification details...")
        classification = self.client.classifications.get_classification_details(classification_id)
        logging.info(f"Retrieved classification: {classification.get('name', 'N/A')}")

        # Generate filename if not provided
        if not filename:
            base_name = classification.get('name', 'classification')
            filename = create_json_filename(f"{base_name}_classification")
            logging.debug(f"Generated filename: {filename}")

        # Save to file
        self.save_classification_to_file(classification, filename, output_dir)

        logging.debug("=" * 80)


    def export_classification_by_name(self, classification_name: str, output_dir: str = "./output", filename: Optional[str] = None):
        """
        Export a single classification by name

        Args:
            classification_name: Classification name to search for
            output_dir: Output directory path
            filename: Custom filename (auto-generated if None)
        """
        logging.debug("=" * 80)
        logging.debug(f"export_classification_by_name() called")
        logging.debug(f"Classification name: {classification_name}")

        # Get all classifications and find matching name
        logging.info("Fetching classifications list...")
        classifications = self.client.classifications.get_all_classifications()
        logging.info(f"Retrieved {len(classifications)} classifications")

        matching = None
        for c in classifications:
            if c.get('name', '').lower() == classification_name.lower():
                matching = c
                break

        if not matching:
            logging.error(f"Classification not found: {classification_name}")
            raise ValueError(f"Classification '{classification_name}' not found")

        logging.info(f"Found classification: {matching['name']} (ID: {matching['id']})")

        # Export using ID
        self.export_classification(matching['id'], output_dir, filename)

        logging.debug("=" * 80)


    def export_all_classifications(self, output_dir: str = "./output"):
        """
        Export all classifications to separate JSON files

        Args:
            output_dir: Output directory path
        """
        logging.debug("=" * 80)
        logging.debug(f"export_all_classifications() called")
        logging.debug(f"Output dir: {output_dir}")

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Get all classifications
        logging.info("Fetching classifications list...")
        classifications = self.client.classifications.get_all_classifications()

        if not classifications:
            logging.warning("No classifications found")
            return

        total = len(classifications)
        logging.info(f"Starting export of {total} classifications")

        success_count = 0
        failed_count = 0

        for idx, classification_summary in enumerate(classifications, 1):
            classification_id = classification_summary.get('id')
            classification_name = classification_summary.get('name', 'unknown')

            logging.info(f"[{idx}/{total}] Exporting: {classification_name}")
            logging.debug(f"Classification ID: {classification_id}")

            try:
                # Fetch full details
                classification = self.client.classifications.get_classification_details(classification_id)

                # Generate filename
                filename = create_json_filename(f"{classification_name}_classification")

                # Save to file
                self.save_classification_to_file(classification, filename, output_dir)

                success_count += 1
                logging.debug(f"Successfully exported {classification_name}")

            except Exception as e:
                failed_count += 1
                logging.error(f"Failed to export {classification_name}: {e}")
                logging.debug(f"Exception details: {type(e).__name__}: {str(e)}", exc_info=True)

        logging.info("=" * 80)
        logging.info(f"Export Summary:")
        logging.info(f"\t- Total: {total}")
        logging.info(f"\t- Success: {success_count}")
        logging.info(f"\t- Failed: {failed_count}")
        logging.info("=" * 80)
        logging.debug("=" * 80)


    def list_classifications(self):
        """
        List all available classifications with their IDs and descriptions
        """
        logging.debug("=" * 80)
        logging.debug("list_classifications() called")

        logging.info("Fetching classifications list...")
        classifications = self.client.classifications.get_all_classifications()

        if not classifications:
            logging.info("No classifications found")
            return

        logging.info("=" * 80)
        logging.info(f"Available Classifications ({len(classifications)} total):")
        logging.info("=" * 80)

        for idx, c in enumerate(classifications, 1):
            name = c.get('name', 'N/A')
            classification_id = c.get('id', 'N/A')
            description = c.get('description', 'No description')

            logging.info(f"{idx}. {name}")
            logging.info(f"   ID: {classification_id}")
            logging.info(f"   Description: {description}")
            logging.info("")

        logging.debug("=" * 80)


def main():
    parser = argparse.ArgumentParser(description='Export IDMC CDGC Classifications to JSON files')
    parser.add_argument('-l', '--list', action='store_true', help='List all available classifications')
    parser.add_argument('-x', '--export-id', help='Classification ID to export')
    parser.add_argument('-n', '--name', help='Classification name to export')
    parser.add_argument('-a', '--all', action='store_true', help='Export all classifications')
    parser.add_argument('-o', '--output', help='Output filename (only for single classification export)')
    parser.add_argument('-d', '--output-dir', default='./output', help='Output directory for JSON files (default: ./output)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')

    args = parser.parse_args()

    # Set logging level
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)-5s - %(message)s', force=True)

    logging.info("=" * 80)
    logging.info("Starting Classification Export")
    logging.info("=" * 80)
    logging.info("Parameters:")
    logging.info(f"\t- List: {args.list}")
    logging.info(f"\t- ID: {args.export_id}")
    logging.info(f"\t- Name: {args.name}")
    logging.info(f"\t- All: {args.all}")
    logging.info(f"\t- Output Dir: {args.output_dir}")
    logging.info(f"\t- Verbose: {args.verbose}")
    logging.debug(f"All args: {vars(args)}")

    # Validate arguments
    if not any([args.list, args.export_id, args.name, args.all]):
        logging.error("Error: Must specify one of: --list, --export-id, --name, or --all")
        parser.print_help()
        sys.exit(1)

    if sum([bool(args.export_id), bool(args.name), args.all, args.list]) > 1:
        logging.error("Error: Only one of --list, --export-id, --name, or --all can be specified")
        sys.exit(1)

    try:
        # Initialize authentication
        logging.info(f"Authenticating user: {username}...")
        logging.debug(f"Login URL: {login_url}")
        auth = IDMCAuth(username=username, password=password, login_url=login_url)
        auth.login()
        auth.generate_jwt_token()
        logging.info(f"Authentication successful for org: {auth.org_name}")
        logging.debug("IDMCAuth object created and authenticated")

        # Create exporter
        logging.debug("Creating ClassificationExporter...")
        logging.debug(f"CDGC API URL: {cdgc_api_url}")
        exporter = ClassificationExporter(auth=auth, cdgc_api_url=cdgc_api_url)
        logging.debug("Exporter created successfully")

        # Execute requested operation
        logging.info("Starting export process...")

        if args.list:
            logging.debug("Listing all classifications")
            exporter.list_classifications()

        elif args.all:
            logging.debug("Exporting all classifications")
            exporter.export_all_classifications(args.output_dir)

        elif args.export_id:
            logging.debug(f"Exporting classification by ID: {args.export_id}")
            exporter.export_classification(args.export_id, args.output_dir, args.output)

        elif args.name:
            logging.debug(f"Exporting classification by name: {args.name}")
            exporter.export_classification_by_name(args.name, args.output_dir, args.output)

        logging.info("=" * 80)
        logging.info("Export completed successfully!")
        logging.info("=" * 80)

        log_usage(auth.org_name, os.path.basename(__file__), "Classification Export")

    except IDMCAuthenticationError as e:
        logging.error("=" * 80)
        logging.error(f"Authentication Error: {e}")
        logging.error("Please check your credentials and login URL.")
        logging.error("=" * 80)
        logging.debug("Authentication error details:", exc_info=True)
        sys.exit(1)

    except Exception as e:
        logging.error("=" * 80)
        logging.error(f"Unexpected Error: {e}")
        logging.error("=" * 80)
        logging.debug("Unexpected error details:", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()