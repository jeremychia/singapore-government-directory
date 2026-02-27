"""Pre-flight checks for the Singapore Government Directory extractor."""

import json
import os

GCP_TOKEN_PATH = "token/gcp_token.json"


class PreflightCheck:
    """Result of a single pre-flight check."""
    
    def __init__(self, name: str, passed: bool, details: list[str] = None, data: dict = None):
        self.name = name
        self.passed = passed
        self.details = details or []
        self.data = data or {}
    
    def print_result(self):
        """Print the check result in a consistent format."""
        status = "[✓]" if self.passed else "[✗]"
        print(f"{status} {self.name}")
        for detail in self.details:
            print(f"    → {detail}")


def _validate_gcp_token(token_path: str) -> PreflightCheck:
    """Validate the GCP service account token file."""
    name = f"GCP token file: {token_path}"
    
    if not os.path.isfile(token_path):
        return PreflightCheck(name, False, [
            "Error: File not found",
            "Download a service account key from Google Cloud Console:",
            "IAM & Admin > Service Accounts > [Select Account] > Keys > Add Key"
        ])
    
    try:
        with open(token_path, 'r') as f:
            token_data = json.load(f)
    except json.JSONDecodeError as e:
        return PreflightCheck(name, False, [f"Error: Invalid JSON format: {e}"])
    except Exception as e:
        return PreflightCheck(name, False, [f"Error: Cannot read file: {e}"])
    
    if token_data.get("type") == "placeholder":
        return PreflightCheck(name, False, [
            "Error: File is a placeholder - replace with actual service account key"
        ])
    
    required_fields = ["type", "project_id", "private_key_id", "private_key",
                       "client_email", "client_id", "auth_uri", "token_uri"]
    missing = [f for f in required_fields if f not in token_data]
    if missing:
        return PreflightCheck(name, False, [f"Error: Missing fields: {', '.join(missing)}"])
    
    if token_data.get("type") != "service_account":
        return PreflightCheck(name, False, [
            f"Error: Invalid type: expected 'service_account', got '{token_data.get('type')}'"
        ])
    
    if not token_data.get("private_key", "").startswith("-----BEGIN PRIVATE KEY-----"):
        return PreflightCheck(name, False, ["Error: Invalid private key format"])
    
    if not token_data.get("client_email", "").endswith(".iam.gserviceaccount.com"):
        return PreflightCheck(name, False, [
            f"Error: Invalid client_email format: {token_data.get('client_email')}"
        ])
    
    return PreflightCheck(name, True, [
        f"Project: {token_data.get('project_id')}",
        f"Service Account: {token_data.get('client_email')}"
    ], data={"token_data": token_data})


def _check_python_packages() -> PreflightCheck:
    """Check if all required Python packages are installed."""
    required = [("beautifulsoup4", "bs4"), ("pandas", "pandas"),
                ("pandas-gbq", "pandas_gbq"), ("requests", "requests")]
    
    missing = []
    for package_name, import_name in required:
        try:
            __import__(import_name)
        except ImportError:
            missing.append(package_name)
    
    if missing:
        return PreflightCheck("Python packages", False, [
            f"Missing: {', '.join(missing)}",
            f"Run: uv pip install {' '.join(missing)}"
        ])
    
    return PreflightCheck("All required Python packages installed", True)


def _check_network_connectivity() -> PreflightCheck:
    """Check network connectivity to sgdi.gov.sg."""
    try:
        import requests
        response = requests.head("https://www.sgdi.gov.sg", timeout=5)
        if response.status_code < 400:
            return PreflightCheck("Network connectivity to sgdi.gov.sg", True)
        return PreflightCheck("Network connectivity to sgdi.gov.sg", False, [
            f"Error: Status code {response.status_code}"
        ])
    except Exception as e:
        return PreflightCheck("Network connectivity to sgdi.gov.sg", False, [
            f"Error: Cannot reach sgdi.gov.sg: {e}"
        ])


def _check_bigquery_connectivity(token_data: dict | None) -> PreflightCheck:
    """Check BigQuery connectivity."""
    try:
        from gbq import PROJECT_ID
        import pandas_gbq
        
        details = []
        if token_data and token_data.get("project_id") != PROJECT_ID:
            details.append(
                f"Warning: Token project '{token_data.get('project_id')}' "
                f"differs from configured project '{PROJECT_ID}'"
            )
        
        pandas_gbq.read_gbq("SELECT 1 as test", project_id=PROJECT_ID)
        details.insert(0, f"Project: {PROJECT_ID}")
        return PreflightCheck("BigQuery connectivity", True, details)
    
    except Exception as e:
        error_msg = str(e)
        details = ["Error: BigQuery connectivity failed"]
        
        if "403" in error_msg or "Permission" in error_msg:
            details.extend([
                "Permission denied - ensure service account has:",
                "  - BigQuery Data Editor",
                "  - BigQuery Job User"
            ])
        elif "404" in error_msg or "not found" in error_msg.lower():
            details.append("Project or dataset not found")
        elif "401" in error_msg or "Unauthorized" in error_msg:
            details.append("Authentication failed - private key may be invalid")
        else:
            details.append(str(e))
        
        return PreflightCheck("BigQuery connectivity", False, details)


def check_requirements(config: dict) -> bool:
    """Run all pre-flight checks and return True if all pass."""
    checks: list[PreflightCheck] = []
    
    print("=" * 60)
    print("Pre-flight checks")
    print("=" * 60)
    
    # Check 1: GCP Token
    gcp_check = _validate_gcp_token(GCP_TOKEN_PATH)
    checks.append(gcp_check)
    gcp_check.print_result()
    
    # Check 2: Python packages
    pkg_check = _check_python_packages()
    checks.append(pkg_check)
    pkg_check.print_result()
    
    # Check 3: Network (only if extraction enabled)
    needs_network = (
        config["run"].get("ministry_extractor", False) or
        config["run"].get("organs_of_state_extractor", False)
    )
    if needs_network:
        net_check = _check_network_connectivity()
        checks.append(net_check)
        net_check.print_result()
    
    # Check 4: BigQuery (only if any operation needs it and GCP token is valid)
    needs_bigquery = any([
        config["run"].get("ministry_extractor", False),
        config["run"].get("organs_of_state_extractor", False),
        config["run"].get("slowly_changing_dimensions", False),
        config["run"].get("name_cleaning", False),
    ])
    if needs_bigquery and gcp_check.passed:
        token_data = gcp_check.data.get("token_data")
        bq_check = _check_bigquery_connectivity(token_data)
        checks.append(bq_check)
        bq_check.print_result()
    
    print("=" * 60)
    all_passed = all(check.passed for check in checks)
    print("All pre-flight checks passed!" if all_passed else 
          "Some checks failed. Please fix the issues above before running.")
    print("=" * 60)
    print()
    
    return all_passed
