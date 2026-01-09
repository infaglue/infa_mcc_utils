# ----------------------------------------------------------------------------------------------------------------------------------------------
# This script looks up a catalog source by name, executes it, and monitors the job completion
# ----------------------------------------------------------------------------------------------------------------------------------------------
import sys
import logging
import argparse
import json
import time
from dotenv import load_dotenv
from cdgc_wrapper import *
from idmc_wrapper import *

# ----------------------------------------------------------------------------------------------------------------------------------------------
# Parameter setup
# ----------------------------------------------------------------------------------------------------------------------------------------------
load_dotenv()
username = os.getenv("INFORMATICA_USERNAME")
password = os.getenv("INFORMATICA_PASSWORD")
login_url = os.getenv("INFORMATICA_LOGIN_URL", "https://dmp-us.informaticacloud.com")
pod_api_url = os.getenv("INFORMATICA_POD_API_URL", "https://usw1.dmp-us.informaticacloud.com")
cdgc_api_url = os.getenv("INFORMATICA_CDGC_API_URL", "https://cdgc-api.dmp-us.informaticacloud.com")


# ----------------------------------------------------------------------------------------------------------------------------------------------
# Functions
# ----------------------------------------------------------------------------------------------------------------------------------------------
class HelpOnErrorParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_help()
        print(f'\nError: {message}', file=sys.stderr)
        sys.exit(2)

def search_catalog_source(cdgc, sourceName):
    """
    Search for a catalog source by name

    Args:
        cdgc: CDGC client instance
        sourceName: Name of the catalog source to search for

    Returns:
        Tuple of (found, catalogSource) where found is boolean and catalogSource is the asset dict
    """

    logging.info(f"Searching for catalog source: {sourceName}")

    # Build search query to find catalog sources with the specified name
    # Using core.classType to filter for catalog sources specifically
    search_query = "*"

    search_body = {
        "from": 0,
        "size": 100,
        "filterSpec": [
            {
                "type": "dsl",
                "expr": f"core.classType core.Resource and core.reference False and core.name '{sourceName}'"
            }
        ]
    }

    try:
        searchResults = cdgc.search.search_assets(
            knowledge_query=search_query,
            segments="all",
            body=search_body
        )

        if searchResults and int(searchResults['summary']['total_hits']) > 0:
            # Look for exact match (case-insensitive)
            for asset in searchResults['hits']:
                if asset['summary']['core.name'].lower() == sourceName.lower():
                    return True, asset

            # If no exact match, return first result
            logging.warning(
                f"No exact match found. Returning first result: {searchResults['hits'][0]['summary']['core.name']}")
            return True, searchResults['hits'][0]

        else:
            logging.warning(f"No catalog source found with name: {sourceName}")
            return False, None

    except Exception as e:
        logging.error(f"Error searching for catalog source: {repr(e)}")
        return False, None


def execute_catalog_source(cdgc, catalogSource, capabilities=None):
    """
    Execute a catalog source scan job

    Args:
        cdgc: CDGC client instance
        catalogSource: The catalog source asset dictionary
        capabilities: Optional list of capabilities to run

    Returns:
        Tuple of (success, job_info) where success is boolean and job_info contains jobId and status
    """

    catalog_source_id = catalogSource['systemAttributes'].get('core.origin')
    catalog_source_name = catalogSource['summary'].get('core.name')

    logging.info(f"Executing catalog source: {catalog_source_name}")
    logging.info(f"Catalog source ID: {catalog_source_id}")

    if capabilities:
        logging.info(f"Running with capabilities: {', '.join(capabilities)}")
    else:
        logging.info("Running with all configured capabilities")

    try:
        # Execute the catalog source
        result = cdgc.catalog_source.run_catalog_source_job(
            catalog_source_id=catalog_source_id,
            capabilities=capabilities
        )

        job_id = result.get('jobId')
        job_uri = result.get('jobUri') or result.get('trackingURI')

        if job_id:
            logging.info(f"Catalog source execution started successfully")
            logging.info(f"Job ID: {job_id}")
            if job_uri:
                logging.info(f"Job URI: {job_uri}")

            return True, {
                'jobId': job_id,
                'jobUri': job_uri,
                'catalogSourceName': catalog_source_name,
                'catalogSourceId': catalog_source_id
            }
        else:
            logging.error("Job ID not found in response")
            return False, None

    except CDGCAPIError as e:
        error_str = str(e)

        # Check if it's a 500 error
        if "API Error 500" in error_str:
            logging.error(f"API Error 500: Server error occurred while executing catalog source")

            # Try to extract the message from the error
            try:
                # Parse the error message to extract JSON content
                import re
                json_match = re.search(r'\{.*\}', error_str)
                if json_match:
                    error_data = json.loads(json_match.group())
                    if 'message' in error_data:
                        logging.error(f"Server message: {error_data['message']}")
                else:
                    logging.error(f"Error details: {error_str}")
            except:
                logging.error(f"Error details: {error_str}")
        else:
            logging.error(f"Error executing catalog source: {repr(e)}")

        return False, None

    except Exception as e:
        logging.error(f"Error executing catalog source: {repr(e)}")
        return False, None


def monitor_job(cdgc, job_info, poll_interval=30, timeout=3600):
    """
    Monitor a catalog source job until completion

    Args:
        cdgc: CDGC client instance
        job_info: Dictionary containing jobId and other job information
        poll_interval: Seconds between status checks (default: 30)
        timeout: Maximum seconds to wait (default: 3600 = 1 hour)

    Returns:
        Tuple of (success, final_status)
    """

    job_id = job_info['jobId']
    catalog_source_name = job_info.get('catalogSourceName', 'Unknown')

    logging.info(f"Monitoring job: {job_id}")
    logging.info(f"Poll interval: {poll_interval} seconds")
    logging.info(f"Timeout: {timeout} seconds")

    try:
        start_time = time.time()

        while True:
            # Check for timeout
            if time.time() - start_time > timeout:
                error_msg = f"Job {job_id} did not complete within {timeout} seconds"
                logging.error(error_msg)
                raise TimeoutError(error_msg)

            # Get current job status
            status = cdgc.jobs.get_job_status(job_id)
            job_state = status.get("status", "").upper()

            logging.info(f"Current job state: {job_state}")

            # Check if job completed successfully
            if job_state in ["COMPLETED", "SUCCESS", "SUCCESSFUL"]:
                logging.info(f"Job completed successfully")
                logging.info(f"Final state: {status.get('state', 'Unknown')}")
                return True, status

            # Check if job completed, but had some errors (e.g. profiling had issues with a table)
            if job_state in ["PARTIAL_COMPLETED"]:
                logging.info(f"Job completed with some errors")
                logging.info(f"Final state: {status.get('state', 'Unknown')}")
                return True, status

            # Check if job failed
            elif job_state in ["FAILED", "ERROR", "CANCELLED"]:
                error_msg = status.get("errorMessage", "Job failed")
                logging.error(f"Job failed: {error_msg}")
                raise CDGCAPIError(f"Job {job_id} failed: {error_msg}")

            # Job still running, wait before next check
            time.sleep(poll_interval)

    except TimeoutError as e:
        logging.error(f"Job monitoring timed out: {str(e)}")
        return False, None

    except CDGCAPIError as e:
        logging.error(f"Job failed: {str(e)}")
        return False, None

    except Exception as e:
        logging.error(f"Error monitoring job: {repr(e)}")
        return False, None


# ----------------------------------------------------------------------------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------------------------------------------------------------------------
def main(argv):

    parser = HelpOnErrorParser(
        description="Execute a catalog source scan job by name and monitor its completion.",
        formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=55, width=125)
    )

    parser.add_argument('-n', '--name', required=False, help='The name of the catalog source to execute.')

    # Capability flags
    for short_flag, long_flag, desc in [
        ('me', 'metadata-extraction', 'Metadata Extraction'),
        ('dp', 'data-profiling', 'Data Profiling'),
        ('dc', 'data-classification', 'Data Classification'),
        ('dq', 'data-quality', 'Data Quality'),
        ('rd', 'relationship-discovery', 'Relationship Discovery'),
        ('ga', 'glossary-association', 'Glossary Association'),
        ('ld', 'lineage-discovery', 'Lineage Discovery')
    ]:
        parser.add_argument(f'-{short_flag}', f'--{long_flag}', action='store_true', help=f'Run {desc} capability.')

    parser.add_argument('-p', '--poll-interval', type=int, default=30, help='Seconds between job status checks (default: 10).')
    parser.add_argument('-t', '--timeout', type=int, default=3600, help='Maximum seconds to wait for job completion (default: 3600).')
    parser.add_argument('--no-wait', action='store_true', help='Do not wait for job completion, just start the job and exit.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging for debugging.')
    parser.add_argument('-j', '--json', action='store_true', help='Output job information in JSON format.')

    args = parser.parse_args()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    sourceName = args.name

    # Build capabilities list from individual flags
    capabilities = []
    if args.metadata_extraction:
        capabilities.append("Metadata Extraction")
    if args.data_profiling:
        capabilities.append("Data Profiling")
    if args.data_classification:
        capabilities.append("Data Classification")
    if args.data_quality:
        capabilities.append("Data Quality")
    if args.relationship_discovery:
        capabilities.append("Relationship Discovery")
    if args.glossary_association:
        capabilities.append("Glossary Association")
    if args.lineage_discovery:
        capabilities.append("Lineage Discovery")

    if not capabilities:
        logging.error("No capabilities provided, please include at least one.")
        sys.exit(1)

    poll_interval = args.poll_interval
    timeout = args.timeout
    no_wait = args.no_wait
    is_verbose = args.verbose
    output_json = args.json

    log_level = logging.DEBUG if is_verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)-5s - %(message)s'
    )

    logging.info(f"Starting")
    logging.info(f"Parameters")
    logging.info(f"\t- Catalog Source Name: {sourceName}")
    if capabilities:
        logging.info(f"\t- Capabilities: {', '.join(capabilities)}")
    if not no_wait:
        logging.info(f"\t- Poll Interval: {poll_interval} seconds")
        logging.info(f"\t- Timeout: {timeout} seconds")

    try:
        # Initialize authentication
        logging.info(f"Authenticating user: {username}...")
        auth = IDMCAuth(username=username, password=password, login_url=login_url)

        # Create CDGC client
        logging.info("Initializing CDGC client...")
        cdgc = CDGC(auth=auth, cdgc_api_url=cdgc_api_url)

        # Search for the catalog source
        found, catalogSource = search_catalog_source(cdgc, sourceName)

        if not found:
            logging.error(f"Catalog source not found: {sourceName}")
            logging.info("Please verify the catalog source name and try again.")
            sys.exit(1)

        logging.info(f"Successfully found catalog source: {catalogSource['summary']['core.name']}")

        # Execute the catalog source
        success, job_info = execute_catalog_source(cdgc, catalogSource, capabilities)

        if not success:
            logging.error("Failed to execute catalog source")
            sys.exit(1)

        # Output job information
        if output_json:
            logging.info("--- Job Information (JSON) ---")
            logging.info(json.dumps(job_info, indent=2))

        # Monitor job if not in no-wait mode
        if not no_wait:
            logging.info("=" * 80)
            logging.info("MONITORING JOB EXECUTION")
            logging.info("=" * 80)

            success, final_status = monitor_job(cdgc, job_info, poll_interval, timeout)

            if success:
                logging.info("=" * 80)
                logging.info("JOB COMPLETED SUCCESSFULLY")
                logging.info("=" * 80)

                if output_json and final_status:
                    logging.info("--- Final Job Status (JSON) ---")
                    logging.info(json.dumps(final_status, indent=2))

            else:
                logging.error("Job monitoring failed or job did not complete successfully")
                sys.exit(1)
        else:
            logging.info("Job started successfully. Exiting without waiting for completion.")
            logging.info(f"You can monitor the job manually using Job ID: {job_info['jobId']}")

        # Log usage
        log_usage(auth.org_name, os.path.basename(__file__), f"Execute Catalog Source: {sourceName}")

    except IDMCAuthenticationError as e:
        logging.error(f"Authentication Error: {e}")
        logging.error("Please check your credentials and login URL.")
        sys.exit(1)

    except CDGCAPIError as e:
        logging.error(f"API Error: {e}")
        logging.error("Please check your API permissions and base URL.")
        sys.exit(1)

    except Exception as e:
        logging.error(f"Unexpected Error: {e}")
        import traceback
        logging.error(traceback.format_exc())
        sys.exit(1)

    logging.info("Script Finished")


if __name__ == "__main__":
    main(sys.argv)