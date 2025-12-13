from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional


class DocumentStatus(str, Enum):
    NEW = "NEW"
    INDEXED = "INDEXED"
    FAILED = "FAILED"


@dataclass
class Document:
    id: int
    file_path: Path
    title: Optional[str]
    venue: Optional[str]
    year: Optional[int]
    tags: list[str]
    status: DocumentStatus
    last_error: Optional[str]
