import argparse
import json
import os
import sys
from typing import Tuple

from logger import get_logger, setup_logging, LogContext
from ministries import ministries_url, organs_of_state_url
from ministries.pipeline import MinistryDataProcessor
from name_cleaning.pipeline import NameProcessorPipeline
from slowly_changing_dimensions.pipeline import ConvertToSCD
from validate_arguments import validate_ministry, validate_organs_of_state

logger = get_logger(__name__)

GCP_TOKEN_PATH = "token/gcp_token.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_TOKEN_PATH
ministry_names = list(ministries_url.keys())
organs_of_states_names = list(organs_of_state_url.keys())


def validate_gcp_token(token_path: str) -> Tuple[bool, str, dict | None]:
    """Validate the GCP service account token file.
    
    Returns:
        Tuple of (is_valid, message, token_data)
    """
    # Check if file exists
    if not os.path.isfile(token_path):
        return False, "File not found", None
    
    # Try to parse as JSON
    try:
        with open(token_path, 'r') as f:
            token_data = json.load(f)
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON format: {e}", None
    except Exception as e:
        return False, f"Cannot read file: {e}", None
    
    # Check for placeholder file
    if token_data.get("type") == "placeholder":
        return False, "File is a placeholder - replace with actual service account key", None
    
    # Check required fields for service account
    required_fields = [
        "type",
        "project_id", 
        "private_key_id",
        "private_key",
        "client_email",
        "client_id",
        "auth_uri",
        "token_uri",
    ]
    
    missing_fields = [field for field in required_fields if field not in token_data]
    if missing_fields:
        return False, f"Missing required fields: {', '.join(missing_fields)}", token_data
    
    # Check that type is service_account
    if token_data.get("type") != "service_account":
        return False, f"Invalid type: expected 'service_account', got '{token_data.get('type')}'", token_data
    
    # Check private key format
    private_key = token_data.get("private_key", "")
    if not private_key.startswith("-----BEGIN PRIVATE KEY-----"):
        return False, "Invalid private key format", token_data
    
    # Check client email format
    client_email = token_data.get("client_email", "")
    if not client_email.endswith(".iam.gserviceaccount.com"):
        return False, f"Invalid client_email format: {client_email}", token_data
    
    return True, "Valid service account key", token_data


def check_requirements(config: dict) -> bool:
    """Check if all requirements are satisfied before running.
    
    Returns True if all checks pass, False otherwise.
    """
    all_checks_passed = True
    token_data = None
    
    print("=" * 60)
    print("Pre-flight checks")
    print("=" * 60)

    # Check 1: GCP Token file - comprehensive validation
    is_valid, message, token_data = validate_gcp_token(GCP_TOKEN_PATH)
    
    if is_valid:
        print(f"[✓] GCP token file valid: {GCP_TOKEN_PATH}")
        print(f"    → Project: {token_data.get('project_id')}")
        print(f"    → Service Account: {token_data.get('client_email')}")
    else:
        print(f"[✗] GCP token file invalid: {GCP_TOKEN_PATH}")
        print(f"    → Error: {message}")
        print("    → Download a service account key from Google Cloud Console:")
        print("      IAM & Admin > Service Accounts > [Select Account] > Keys > Add Key")
        all_checks_passed = False

    # Check 2: Required Python packages
    missing_packages = []
    required_packages = [
        ("beautifulsoup4", "bs4"),
        ("pandas", "pandas"),
        ("pandas-gbq", "pandas_gbq"),
        ("requests", "requests"),
    ]
    
    for package_name, import_name in required_packages:
        try:
            __import__(import_name)
        except ImportError:
            missing_packages.append(package_name)
    
    if missing_packages:
        print(f"[✗] Missing Python packages: {', '.join(missing_packages)}")
        print("    → Run: uv pip install " + " ".join(missing_packages))
        all_checks_passed = False
    else:
        print("[✓] All required Python packages are installed")

    # Check 3: Network connectivity (only if extraction is enabled)
    needs_network = (
        config["run"].get("ministry_extractor", False) or 
        config["run"].get("organs_of_state_extractor", False)
    )
    
    if needs_network:
        try:
            import requests
            response = requests.head("https://www.sgdi.gov.sg", timeout=5)
            if response.status_code < 400:
                print("[✓] Network connectivity to sgdi.gov.sg confirmed")
            else:
                print(f"[✗] sgdi.gov.sg returned status code: {response.status_code}")
                all_checks_passed = False
        except Exception as e:
            print(f"[✗] Cannot reach sgdi.gov.sg: {e}")
            all_checks_passed = False

    # Check 4: BigQuery connectivity (only if we have a valid token)
    needs_bigquery = any([
        config["run"].get("ministry_extractor", False),
        config["run"].get("organs_of_state_extractor", False),
        config["run"].get("slowly_changing_dimensions", False),
        config["run"].get("name_cleaning", False),
    ])
    
    if needs_bigquery and is_valid:
        try:
            from gbq import PROJECT_ID
            import pandas_gbq
            
            # Verify project ID matches
            if token_data and token_data.get("project_id") != PROJECT_ID:
                print(f"[!] Warning: Token project '{token_data.get('project_id')}' differs from configured project '{PROJECT_ID}'")
            
            # Test BigQuery connectivity with a simple query
            pandas_gbq.read_gbq("SELECT 1 as test", project_id=PROJECT_ID)
            print(f"[✓] BigQuery connectivity confirmed (project: {PROJECT_ID})")
        except Exception as e:
            error_msg = str(e)
            print(f"[✗] BigQuery connectivity failed")
            
            # Provide specific guidance based on error type
            if "403" in error_msg or "Permission" in error_msg:
                print("    → Error: Permission denied")
                print("    → Ensure the service account has these roles:")
                print("      - BigQuery Data Editor")
                print("      - BigQuery Job User")
            elif "404" in error_msg or "not found" in error_msg.lower():
                print("    → Error: Project or dataset not found")
                print(f"    → Verify project '{PROJECT_ID}' exists and is accessible")
            elif "401" in error_msg or "Unauthorized" in error_msg:
                print("    → Error: Authentication failed")
                print("    → The private key may be invalid or expired")
            else:
                print(f"    → Error: {e}")
            
            all_checks_passed = False

    print("=" * 60)
    
    if all_checks_passed:
        print("All pre-flight checks passed!")
    else:
        print("Some checks failed. Please fix the issues above before running.")
    
    print("=" * 60)
    print()
    
    return all_checks_passed


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Singapore Government Directory Extracter"
    )

    parser.add_argument(
        "--ministry_extractor",
        "-me",
        action="store_true",
        required=False,
        help="Use this flag to run the ministry extractor",
    )

    parser.add_argument(
        "--ministry",
        "-m",
        nargs="+",
        type=validate_ministry,
        required=False,
        help="Key in full name of ministry to extract from. If blank, extracts from all ministries.",
    )

    parser.add_argument(
        "--organs_of_state_extractor",
        "-oose",
        action="store_true",
        required=False,
        help="Use this flag to run the organs of state extractor",
    )

    parser.add_argument(
        "--organs_of_state",
        "-oos",
        nargs="+",
        type=validate_organs_of_state,
        required=False,
        help="Key in full name of organs of state to extract from. If blank, extracts from all organs of state.",
    )

    parser.add_argument(
        "--resume_run",
        "-rr",
        action="store_true",
        required=False,
        help="Used when a run breaks and is to be continued",
    )

    parser.add_argument(
        "--slowly_changing_dimensions",
        "-scd",
        action="store_true",
        required=False,
        help="Use this flag to run the slowly changing dimension convertor",
    )

    parser.add_argument(
        "--name_cleaning",
        "-nc",
        action="store_true",
        required=False,
        help="Use this flag to run the name cleaner",
    )

    parser.add_argument(
        "--check",
        "-c",
        action="store_true",
        required=False,
        help="Run pre-flight checks only (validate requirements without executing)",
    )

    parser.add_argument(
        "--skip_checks",
        action="store_true",
        required=False,
        help="Skip pre-flight checks and run directly",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        required=False,
        help="Enable verbose/debug logging output",
    )

    return parser.parse_args()


def initialise_config(args: argparse.Namespace) -> Tuple[str, dict]:
    config = {
        "run": {},
        "ministries": ministry_names if ministry_names else [],
        "organs_of_state": organs_of_states_names if organs_of_states_names else [],
    }

    message = "Configurations:\n"

    # Ministry Extractor flag
    if args.ministry_extractor:
        config["run"]["ministry_extractor"] = True
        message += "[y]\twill run ministry_extractor\n"

        if args.ministry:
            if args.resume_run:
                config["ministries"] = ministry_names[
                    ministry_names.index(args.ministry[0]) :
                ]
                message += (
                    "[y]\tministries specified with resume run, "
                    f"will run from {args.ministry[0]}, i.e.: {config['ministries']}\n"
                )
            else:
                config["ministries"] = args.ministry
                message += (
                    f"[y]\tministries specified, will run {config['ministries']}\n"
                )

        elif ministry_names:
            message += "[y]\tno ministries specified, will run all\n"
        else:
            message += "[n]\tno ministries specified, no default available\n"

    else:
        config["run"]["ministry_extractor"] = False
        message += "[n]\tno ministry extraction\n"
        if args.ministry:
            message += "[!]\tno extraction will be run without the flag --ministry_extractor or -e\n"

    # Organs of State Extractor flag
    if args.organs_of_state_extractor:
        config["run"]["organs_of_state_extractor"] = True
        message += "[y]\twill run organs_of_state_extractor\n"

        if args.organs_of_state:
            if args.resume_run:
                config["organs_of_state"] = organs_of_states_names[
                    organs_of_states_names.index(args.organs_of_state[0]) :
                ]
                message += (
                    "[y]\torgans_of_state specified with resume run, "
                    f"will run from {args.organs_of_state[0]}, i.e.: {config['organs_of_state']}\n"
                )
            else:
                config["organs_of_state"] = args.ministry
                message += (
                    f"[y]\tministries specified, will run {config['organs_of_state']}\n"
                )

        elif organs_of_states_names:
            message += "[y]\tno organs_of_state specified, will run all\n"
        else:
            message += "[n]\tno organs_of_state specified, no default available\n"

    else:
        config["run"]["organs_of_state_extractor"] = False
        message += "[n]\tno organs_of_state extraction\n"
        if args.ministry:
            message += "[!]\tno extraction will be run without the flag --organs_of_state_extractor or -e\n"

    # SCD flag
    if args.slowly_changing_dimensions:
        config["run"]["slowly_changing_dimensions"] = True
        message += "[y]\twill perform SCD processing\n"
    else:
        config["run"]["slowly_changing_dimensions"] = False
        message += "[y]\tno SCD processing\n"

    # Name cleaning flag
    if args.name_cleaning:
        config["run"]["name_cleaning"] = True
        message += "[y]\twill perform name cleaning\n"
    else:
        config["run"]["name_cleaning"] = False
        message += "[n]\tno name cleaning\n"

    return message, config


def main():
    args = parse_arguments()
    
    # Initialize logging early
    setup_logging(verbose=getattr(args, 'verbose', False))
    
    message, config = initialise_config(args)
    logger.debug(f"Configuration initialized: {config}")

    # If --check flag is used, only run pre-flight checks
    if args.check:
        check_requirements(config)
        return

    # Run pre-flight checks unless --skip_checks is specified
    if not args.skip_checks:
        if not check_requirements(config):
            logger.error("Aborting due to failed pre-flight checks.")
            print("Use --skip_checks to bypass this check (not recommended).")
            sys.exit(1)

    if config["run"]["ministry_extractor"]:
        for ministry_name, url in ministries_url.items():
            if ministry_name not in config["ministries"]:
                continue
            else:
                logger.info(f"Starting extraction for: {ministry_name}")
                logger.debug(f"URL: {url}")
                with LogContext(logger, "Ministry extraction", ministry=ministry_name):
                    pipeline = MinistryDataProcessor(ministry_name, url)
                    pipeline.process_and_upload()

    if config["run"]["organs_of_state_extractor"]:
        for organ_of_state_name, url in organs_of_state_url.items():
            if organ_of_state_name not in config["organs_of_state"]:
                continue
            else:
                logger.info(f"Starting extraction for: {organ_of_state_name}")
                logger.debug(f"URL: {url}")
                with LogContext(logger, "Organ of State extraction", organ=organ_of_state_name):
                    pipeline = MinistryDataProcessor(organ_of_state_name, url)
                    pipeline.process_and_upload()

    if config["run"]["slowly_changing_dimensions"]:
        with LogContext(logger, "SCD processing"):
            convert_scd = ConvertToSCD()
            convert_scd.process_and_upload()

    if config["run"]["name_cleaning"]:
        with LogContext(logger, "Name cleaning"):
            cleaning = NameProcessorPipeline()
            cleaning.run()

    logger.info("All operations completed successfully")


if __name__ == "__main__":
    main()
