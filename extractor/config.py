"""Configuration dataclasses for the Singapore Government Directory extractor."""

from dataclasses import dataclass, field


@dataclass
class RunConfig:
    """Configuration for which operations to run."""
    ministry_extractor: bool = False
    organs_of_state_extractor: bool = False
    slowly_changing_dimensions: bool = False
    name_cleaning: bool = False


@dataclass
class Config:
    """Main configuration for the extractor."""
    run: RunConfig = field(default_factory=RunConfig)
    ministries: list[str] = field(default_factory=list)
    organs_of_state: list[str] = field(default_factory=list)
