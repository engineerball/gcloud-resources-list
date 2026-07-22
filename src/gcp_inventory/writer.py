"""CSV output for a single inventory run.

Rows are staged under ``output/.in-progress-<run_id>/`` and atomically
published to ``output/<run_id>/`` on success, with a ``latest`` symlink.
A crashed run never corrupts or half-overwrites previous output.
"""

from __future__ import annotations

import csv
import os
import re
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import IO, TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    from gcp_inventory.registry import CollectorSpec

_INTEGER_TEXT = re.compile(r"[+-]?\d+")
_LINE_BREAKS = re.compile(r"[\r\n]+")
_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r", "\n")


def sanitize_csv_cell(value: object) -> object:
    """Neutralize spreadsheet formulas and row breaks in textual CSV cells."""
    if not isinstance(value, str):
        return value

    sanitized = _LINE_BREAKS.sub(" ", value)
    first_non_whitespace = sanitized.lstrip()
    starts_with_control = bool(value) and (ord(value[0]) < 32 or ord(value[0]) == 127)
    # Canonical integer text cannot be a formula, so negative counts stay readable.
    needs_formula_escape = (
        starts_with_control or first_non_whitespace.startswith(_FORMULA_PREFIXES)
    ) and not _INTEGER_TEXT.fullmatch(sanitized)
    if needs_formula_escape:
        return f"'{sanitized}"
    return sanitized


class RunWriter:
    """Owns every CSV file for one run. Not thread-safe by design:
    only the coordinating thread writes."""

    def __init__(self, output_root: str | Path, run_id: str) -> None:
        self.output_root = Path(output_root)
        self.run_id = run_id
        self._staging = self.output_root / f".in-progress-{run_id}"
        self.output_root.mkdir(parents=True, exist_ok=True)
        self._staging.mkdir(exist_ok=False)
        self._files: dict[str, IO[str]] = {}
        self._writers: dict[str, csv._writer] = {}  # type: ignore[name-defined]

    @property
    def staging_dir(self) -> Path:
        """Staging directory; contents move to ``output/<run_id>/`` on finalize."""
        return self._staging

    def write(self, spec: CollectorSpec, rows: Iterable[Sequence[object]]) -> int:
        """Append *rows* to the CSV for *spec*; create file + header on first use.

        Returns the number of rows written.
        """
        if spec.filename not in self._writers:
            fh = (self._staging / spec.filename).open("w", newline="")
            self._files[spec.filename] = fh
            writer = csv.writer(fh)
            writer.writerow(sanitize_csv_cell(cell) for cell in spec.header)
            self._writers[spec.filename] = writer
        writer = self._writers[spec.filename]
        count = 0
        for row in rows:
            writer.writerow(sanitize_csv_cell(cell) for cell in row)
            count += 1
        return count

    def finalize(self) -> Path:
        """Close all files, publish the staging dir, update ``latest``."""
        self.close()
        final = self.output_root / self.run_id
        self._staging.rename(final)
        latest = self.output_root / "latest"
        if latest.exists() and not latest.is_symlink():
            raise FileExistsError(f"Cannot update {latest}: path exists and is not a symlink")

        temporary_latest = self.output_root / f".latest-{uuid4().hex}"
        try:
            temporary_latest.symlink_to(self.run_id)
            os.replace(temporary_latest, latest)
        finally:
            if temporary_latest.is_symlink():
                temporary_latest.unlink()
        return final

    def close(self) -> None:
        """Close open file handles; staging dir is left for inspection."""
        for fh in self._files.values():
            fh.close()
        self._files.clear()
        self._writers.clear()
