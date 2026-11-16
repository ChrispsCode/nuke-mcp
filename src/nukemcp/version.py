"""Version detection and tool gating for Nuke variants and versions."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class NukeVersion:
    major: int
    minor: int
    patch: str  # e.g., "v1", "v5"
    variant: str  # "Nuke", "NukeX", "NukeStudio"
    # False when the server started before Nuke was reachable. Gating is then
    # permissive (all tools registered) and the addon reports anything unsupported.
    known: bool = True

    @classmethod
    def unknown(cls) -> NukeVersion:
        return cls(major=0, minor=0, patch="v0", variant="Nuke", known=False)

    @property
    def is_nukex(self) -> bool:
        if not self.known:
            return True
        return self.variant in ("NukeX", "NukeStudio")

    @property
    def is_studio(self) -> bool:
        return self.variant == "NukeStudio"

    def at_least(self, major: int, minor: int = 0) -> bool:
        if not self.known:
            return True
        return (self.major, self.minor) >= (major, minor)

    def __str__(self) -> str:
        if not self.known:
            return "Nuke (version unknown, not yet connected)"
        return f"{self.variant} {self.major}.{self.minor}{self.patch}"


def parse_version(handshake: dict) -> NukeVersion:
    """Parse a Nuke version from the addon handshake data.

    Expected handshake format:
        {"nuke_version": "17.0v1", "variant": "NukeX", ...}
    """
    version_str = handshake.get("nuke_version")
    if not version_str:
        raise ValueError("Handshake missing 'nuke_version'")
    variant = handshake.get("variant", "Nuke")

    match = re.match(r"(\d+)\.(\d+)(v\d+)?", version_str)
    if not match:
        raise ValueError(f"Could not parse Nuke version string: {version_str!r}")

    return NukeVersion(
        major=int(match.group(1)),
        minor=int(match.group(2)),
        patch=match.group(3) or "v0",
        variant=variant,
    )
