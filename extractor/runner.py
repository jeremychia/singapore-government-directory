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


def _run_extractions(
    items_to_run: list[str],
    url_map: dict[str, str],
    context_name: str,
    context_key: str,
) -> None:
    """Run extraction for multiple entities.
    
    Args:
        items_to_run: List of entity names to extract
        url_map: Mapping of entity names to URLs
        context_name: Name for log context (e.g., "Ministry extraction")
        context_key: Key for context kwargs (e.g., "ministry" or "organ")
    """
    for name in items_to_run:
        url = url_map.get(name)
        if url:
            _run_extraction(name, url, context_name, **{context_key: name})
        else:
            logger.warning(f"No URL found for: {name}")


def run_ministry_extraction(config: Config) -> None:
    """Run extraction for all configured ministries."""
    _run_extractions(
        config.ministries,
        ministries_url,
        "Ministry extraction",
        "ministry",
    )


def run_organs_of_state_extraction(config: Config) -> None:
    """Run extraction for all configured organs of state."""
    _run_extractions(
        config.organs_of_state,
        organs_of_state_url,
        "Organ of State extraction",
        "organ",
    )


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
