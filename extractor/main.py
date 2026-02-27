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


def initialise_config(args) -> tuple[str, Config]:
    config = Config(
        run=RunConfig(),
        ministries=ministry_names.copy() if ministry_names else [],
        organs_of_state=organs_of_states_names.copy() if organs_of_states_names else [],
    )

    message = "Configurations:\n"

    # Ministry Extractor flag
    if args.ministry_extractor:
        config.run.ministry_extractor = True
        message += "[y]\twill run ministry_extractor\n"

        if args.ministry:
            if args.resume_run:
                config.ministries = ministry_names[
                    ministry_names.index(args.ministry[0]) :
                ]
                message += (
                    "[y]\tministries specified with resume run, "
                    f"will run from {args.ministry[0]}, i.e.: {config.ministries}\n"
                )
            else:
                config.ministries = args.ministry
                message += (
                    f"[y]\tministries specified, will run {config.ministries}\n"
                )

        elif ministry_names:
            message += "[y]\tno ministries specified, will run all\n"
        else:
            message += "[n]\tno ministries specified, no default available\n"

    else:
        message += "[n]\tno ministry extraction\n"
        if args.ministry:
            message += "[!]\tno extraction will be run without the flag --ministry_extractor or -e\n"

    # Organs of State Extractor flag
    if args.organs_of_state_extractor:
        config.run.organs_of_state_extractor = True
        message += "[y]\twill run organs_of_state_extractor\n"

        if args.organs_of_state:
            if args.resume_run:
                config.organs_of_state = organs_of_states_names[
                    organs_of_states_names.index(args.organs_of_state[0]) :
                ]
                message += (
                    "[y]\torgans_of_state specified with resume run, "
                    f"will run from {args.organs_of_state[0]}, i.e.: {config.organs_of_state}\n"
                )
            else:
                config.organs_of_state = args.organs_of_state
                message += (
                    f"[y]\torgans_of_state specified, will run {config.organs_of_state}\n"
                )

        elif organs_of_states_names:
            message += "[y]\tno organs_of_state specified, will run all\n"
        else:
            message += "[n]\tno organs_of_state specified, no default available\n"

    else:
        message += "[n]\tno organs_of_state extraction\n"
        if args.ministry:
            message += "[!]\tno extraction will be run without the flag --organs_of_state_extractor or -e\n"

    # SCD flag
    if args.slowly_changing_dimensions:
        config.run.slowly_changing_dimensions = True
        message += "[y]\twill perform SCD processing\n"
    else:
        message += "[y]\tno SCD processing\n"

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
