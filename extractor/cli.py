"""Command-line argument parsing for the Singapore Government Directory extractor."""

import argparse
from validate_arguments import validate_ministry, validate_organs_of_state


# Argument definitions as data
ARGUMENTS = [
    {
        "flags": ["--ministry_extractor", "-me"],
        "action": "store_true",
        "help": "Use this flag to run the ministry extractor",
    },
    {
        "flags": ["--ministry", "-m"],
        "nargs": "+",
        "type": validate_ministry,
        "help": "Full ministry name(s) to extract. If blank, extracts from all ministries.",
    },
    {
        "flags": ["--organs_of_state_extractor", "-oose"],
        "action": "store_true",
        "help": "Use this flag to run the organs of state extractor",
    },
    {
        "flags": ["--organs_of_state", "-oos"],
        "nargs": "+",
        "type": validate_organs_of_state,
        "help": "Full organ(s) of state name to extract. If blank, extracts from all organs of state.",
    },
    {
        "flags": ["--resume_run", "-rr"],
        "action": "store_true",
        "help": "Resume from the first specified ministry/organ after an interrupted run",
    },
    {
        "flags": ["--slowly_changing_dimensions", "-scd"],
        "action": "store_true",
        "help": "Run the Slowly Changing Dimensions (SCD) pipeline",
    },
    {
        "flags": ["--name_cleaning", "-nc"],
        "action": "store_true",
        "help": "Run the name cleaning and standardization pipeline",
    },
    {
        "flags": ["--check", "-c"],
        "action": "store_true",
        "help": "Run pre-flight checks only (validate requirements without executing)",
    },
    {
        "flags": ["--skip_checks"],
        "action": "store_true",
        "help": "Skip pre-flight checks and run directly",
    },
    {
        "flags": ["--verbose", "-v"],
        "action": "store_true",
        "help": "Enable verbose/debug logging output",
    },
]


def parse_arguments() -> argparse.Namespace:
    """Build and parse command-line arguments for extractor workflows."""
    parser = argparse.ArgumentParser(
        description="Singapore Government Directory Extractor"
    )

    for arg in ARGUMENTS:
        flags = arg.pop("flags")
        parser.add_argument(*flags, required=False, **arg)
        arg["flags"] = flags  # Restore for potential reuse

    return parser.parse_args()
