import argparse
import os
from typing import Tuple

from ministries import ministries_url
from ministries.pipeline import MinistryDataProcessor
from name_cleaning.pipeline import NameProcessorPipeline
from validate_arguments import validate_ministry

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "token/gcp_token.json"
ministry_names = list(ministries_url.keys())


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Singapore Government Directory Extracter"
    )

    parser.add_argument(
        "--extractor",
        "-e",
        action="store_true",
        required=False,
        help="Use this flag to run the name cleaner",
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
        "--resume_run",
        "-rr",
        action="store_true",
        required=False,
        help="Used when a run breaks and is to be continued",
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
    }

    message = "Configurations:\n"

    # Extractor flag
    if args.extractor:
        config["run"]["extractor"] = True
        message += "[y]\twill run extractor\n"

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
        config["run"]["extractor"] = False
        message += "[n]\tno extraction\n"
        if args.ministry:
            message += (
                "[!]\tno extraction will be run without the flag --extractor or -e\n"
            )

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

    if config["run"]["extractor"]:
        for ministry_name, url in ministries_url.items():
            print(message)
            print(
                f"starting extraction for:\t{ministry_name}\n" f"from url:\t\t\t{url}\n"
            )
            pipeline = MinistryDataProcessor(ministry_name, url)
            pipeline.process_and_upload()

    if config["run"]["name_cleaning"]:
        print("running name cleaner: ...")
        cleaning = NameProcessorPipeline()
        cleaning.run()
