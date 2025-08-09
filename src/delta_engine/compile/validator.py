from dataclasses import dataclass
from typing import Sequence
from src.delta_engine.state.snapshot import CatalogState


@dataclass(frozen=True)
class PreflightValidator:
    ... 