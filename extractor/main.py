import os
import sys

from cli import parse_arguments
from config import Config, RunConfig
from logger import get_logger, setup_logging
from ministries import ministries_url, organs_of_state_url
from preflight import check_requirements, GCP_TOKEN_PATH
from runner import run_all

logger = get_logger(__name__)

ministry_names = list(ministries_url.keys())
organs_of_states_names = list(organs_of_state_url.keys())


def _configure_extractor(
    args,
    extractor_flag: bool,
    items_arg: list | None,
    all_items: list[str],
    name: str,
    resume_run: bool = False,
) -> tuple[bool, list[str], str]:
    """Configure an extractor (ministry or organs of state).
    
    Returns:
        Tuple of (enabled, items_to_run, message)
    """
    message = ""
    items = all_items.copy() if all_items else []
    
    if extractor_flag:
        message += f"[y]\twill run {name}_extractor\n"
        
        if items_arg:
            if resume_run:
                items = all_items[all_items.index(items_arg[0]):]
                message += (
                    f"[y]\t{name} specified with resume run, "
                    f"will run from {items_arg[0]}, i.e.: {items}\n"
                )
            else:
                items = items_arg
                message += f"[y]\t{name} specified, will run {items}\n"
        elif all_items:
            message += f"[y]\tno {name} specified, will run all\n"
        else:
            message += f"[n]\tno {name} specified, no default available\n"
        
        return True, items, message
    else:
        message += f"[n]\tno {name} extraction\n"
        if items_arg:
            message += f"[!]\tno extraction will be run without the flag --{name}_extractor\n"
        return False, items, message


def initialise_config(args) -> tuple[str, Config]:
    config = Config(run=RunConfig())
    message = "Configurations:\n"

    # Ministry Extractor
    enabled, config.ministries, msg = _configure_extractor(
        args,
        extractor_flag=args.ministry_extractor,
        items_arg=args.ministry,
        all_items=ministry_names,
        name="ministry",
        resume_run=args.resume_run,
    )
    config.run.ministry_extractor = enabled
    message += msg

    # Organs of State Extractor
    enabled, config.organs_of_state, msg = _configure_extractor(
        args,
        extractor_flag=args.organs_of_state_extractor,
        items_arg=args.organs_of_state,
        all_items=organs_of_states_names,
        name="organs_of_state",
        resume_run=args.resume_run,
    )
    config.run.organs_of_state_extractor = enabled
    message += msg

    # SCD flag
    if args.slowly_changing_dimensions:
        config.run.slowly_changing_dimensions = True
        message += "[y]\twill perform SCD processing\n"
    else:
        message += "[n]\tno SCD processing\n"

    # Name cleaning flag
    if args.name_cleaning:
        config.run.name_cleaning = True
        message += "[y]\twill perform name cleaning\n"
    else:
        message += "[n]\tno name cleaning\n"

    return message, config


def main():
    # Set GCP credentials before any GCP operations
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_TOKEN_PATH
    
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

    run_all(config)


if __name__ == "__main__":
    main()
