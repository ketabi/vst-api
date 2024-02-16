from enum import Enum


class IndexingResult(Enum):
    CREATED = 1
    UPDATED = 2
    DELETED = 3
    IGNORED = 4
