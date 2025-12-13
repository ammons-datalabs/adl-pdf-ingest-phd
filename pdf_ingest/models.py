from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional


class DocumentStatus(str, Enum):
    NEW = "NEW"
    PROCESSING = "PROCESSING"
    INDEXED = "INDEXED"
    FAILED = "FAILED"


class JobStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    DONE = "DONE"
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


@dataclass
class Job:
    id: int
    document_id: int
    status: JobStatus
    created_at: datetime
    updated_at: datetime
    attempts: int
    last_error: Optional[str]
