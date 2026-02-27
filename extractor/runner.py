"""Execution runners for the Singapore Government Directory extractor."""

from config import Config
from logger import get_logger, LogContext
from ministries import ministries_url, organs_of_state_url
from ministries.pipeline import MinistryDataProcessor
from name_cleaning.pipeline import NameProcessorPipeline
from slowly_changing_dimensions.pipeline import ConvertToSCD

logger = get_logger(__name__)


def _run_extraction(name: str, url: str, context_name: str, **context_kwargs) -> None:
    """Run extraction for a single entity (ministry or organ of state)."""
    logger.info(f"Starting extraction for: {name}")
    logger.debug(f"URL: {url}")
    with LogContext(logger, context_name, **context_kwargs):
        pipeline = MinistryDataProcessor(name, url)
        pipeline.process_and_upload()


def run_ministry_extraction(config: Config) -> None:
    """Run extraction for all configured ministries."""
    for ministry_name, url in ministries_url.items():
        if ministry_name in config.ministries:
            _run_extraction(ministry_name, url, "Ministry extraction", ministry=ministry_name)


def run_organs_of_state_extraction(config: Config) -> None:
    """Run extraction for all configured organs of state."""
    for organ_name, url in organs_of_state_url.items():
        if organ_name in config.organs_of_state:
            _run_extraction(organ_name, url, "Organ of State extraction", organ=organ_name)


def run_scd_processing() -> None:
    """Run slowly changing dimensions processing."""
    with LogContext(logger, "SCD processing"):
        convert_scd = ConvertToSCD()
        convert_scd.process_and_upload()


def run_name_cleaning() -> None:
    """Run name cleaning processing."""
    with LogContext(logger, "Name cleaning"):
        cleaning = NameProcessorPipeline()
        cleaning.run()


def run_all(config: Config) -> None:
    """Run all configured operations."""
    if config.run.ministry_extractor:
        run_ministry_extraction(config)

    if config.run.organs_of_state_extractor:
        run_organs_of_state_extraction(config)

    if config.run.slowly_changing_dimensions:
        run_scd_processing()

    if config.run.name_cleaning:
        run_name_cleaning()

    logger.info("All operations completed successfully")
