import argparse
import os
from typing import Tuple

from ministries import ministries_url, organs_of_state_url
from ministries.pipeline import MinistryDataProcessor
from name_cleaning.pipeline import NameProcessorPipeline
from slowly_changing_dimensions.pipeline import ConvertToSCD
from validate_arguments import validate_ministry, validate_organs_of_state

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "token/gcp_token.json"
ministry_names = list(ministries_url.keys())
organs_of_states_names = list(organs_of_state_url.keys())


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
                    "[y]\organs_of_state specified with resume run, "
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


if __name__ == "__main__":
    args = parse_arguments()
    message, config = initialise_config(args)

    if config["run"]["ministry_extractor"]:
        for ministry_name, url in ministries_url.items():
            if ministry_name not in config["ministries"]:
                continue
            else:
                print(message)
                print(
                    f"starting extraction for:\t{ministry_name}\n"
                    f"from url:\t\t\t{url}\n"
                )
                pipeline = MinistryDataProcessor(ministry_name, url)
                pipeline.process_and_upload()

    if config["run"]["organs_of_state_extractor"]:
        for organ_of_state_name, url in organs_of_state_url.items():
            if organ_of_state_name not in config["organs_of_state"]:
                continue
            else:
                print(message)
                print(
                    f"starting extraction for:\t{organ_of_state_name}\n"
                    f"from url:\t\t\t{url}\n"
                )
                pipeline = MinistryDataProcessor(organ_of_state_name, url)
                pipeline.process_and_upload()

    if config["run"]["slowly_changing_dimensions"]:
        print("running SCD processor: ...")
        convert_scd = ConvertToSCD()
        convert_scd.process_and_upload()

    if config["run"]["name_cleaning"]:
        print("running name cleaner: ...")
        cleaning = NameProcessorPipeline()
        cleaning.run()
