"""Search <-> client attribution: the draft, and the authority.

Ijon's original plan categorised searches by a `Client_Purpose_Other`
naming convention. Measured against the real account, that does not work:
of 88 searches only 3 names contain underscores (none as field
separators), 9 names are null, and several are placeholders. So VaynerX
owns a mapping table and these helpers only produce a *draft* of it.

The guessing half (`guess_client`, `looks_like_junk`) is a hint for a
human, never an authority. The cost of a wrong guess that looks confident
is higher than the cost of a blank cell, because a blank cell gets filled
in and a wrong guess gets shipped.

The registry half (`parse_client_map`, `resolve_client`) is the opposite:
it reads the finished table a human signed off on, and it is the only
thing callers are allowed to trust. It validates hard and refuses to
return a partial map, because the failure it prevents - one client's
mentions exported under another client's token - is a client-firewall
breach, not a data-quality blemish.
"""

from __future__ import annotations

import dataclasses
import json
import pathlib
import re
from typing import Any

# Names that are obviously not client deliverables.
PLACEHOLDER_NAMES = frozenset(
    {
        "test",
        "general",
        "eating",
        "demo",
        "tmp",
        "temp",
        "asdf",
        "untitled",
        "new search",
        "copy",
        "example",
        "sample",
        "aaa",
        "xxx",
    }
)

# `test`/`testing`, a trailing version marker, or internal training work.
_TEST_TOKEN = re.compile(r"\btest(ing|s)?\b|\bv\d+\s*$|\btraining\b", re.IGNORECASE)

# Separators actually used in this account, in priority order. Note that
# " - " dominates; "_" (the proposed convention) is vanishingly rare.
_SEPARATORS = (" - ", " | ", " x ", "_", ":")

# Below this share of vowels a single alphabetic token reads as keyboard
# mash ("hjknlm", "bgtbrfvdcs") rather than a brand ("Pepsi", "Clif").
_MIN_VOWEL_RATIO = 0.2

_MAX_CLIENT_LEN = 30


def looks_like_junk(name: str | None) -> bool:
    """Heuristic flag for a search that is probably not client work.

    Must not fire on real brands: a false positive on Pepsi - the largest
    search in the account at 5.9M mentions - would mislead the audit far
    more than a missed `hjknlm` would. Flags only; never auto-excludes.
    """
    if not name or not name.strip():
        return True

    cleaned = name.strip()

    if cleaned.lower() in PLACEHOLDER_NAMES:
        return True

    if _TEST_TOKEN.search(cleaned):
        return True

    if " " not in cleaned and len(cleaned) >= 4 and cleaned.isalpha():
        vowels = sum(char in "aeiouAEIOU" for char in cleaned)
        if vowels / len(cleaned) < _MIN_VOWEL_RATIO:
            return True

    return False


def guess_client(name: str | None) -> str:
    """Best-effort client name. Returns "" when there is no safe guess."""
    if not name or not name.strip():
        return ""

    cleaned = name.strip()

    for separator in _SEPARATORS:
        if separator in cleaned:
            head = cleaned.split(separator)[0].strip()
            return head if 2 <= len(head) <= _MAX_CLIENT_LEN else ""

    # A short standalone name is usually the brand itself.
    if len(cleaned.split()) <= 2 and len(cleaned) <= _MAX_CLIENT_LEN:
        return cleaned

    return ""


def is_extractable(
    *,
    mapped: bool,
    active: bool,
    total_contents: int | None,
    status: str | None,
) -> bool:
    """The eligibility gate for the daily run.

    Unmapped searches are quarantined rather than extracted - that is what
    makes the naming-convention ask enforceable without depending on it.
    """
    extractable_statuses = {"READY", "EDIT", "PREPARED"}
    return (
        mapped
        and active
        and bool(total_contents)
        and (status or "").upper() in extractable_statuses
    )


# ---------------------------------------------------------------------------
# the registry - the finished table, not the draft
# ---------------------------------------------------------------------------

# A client token ends up in an S3 key and in a filename a downstream router
# splits on `_`, so it cannot contain the separator and must be stable run
# to run. Lower-case, digits, hyphens.
CLIENT_TOKEN = re.compile(r"^[a-z0-9][a-z0-9-]*$")

AGENCIES = frozenset({"VM", "CM", "TG"})

# Default location of the table inside the repo. The Lambda reads its copy
# from S3 instead; see `load_client_map`.
DEFAULT_MAP_PATH = (
    pathlib.Path(__file__).resolve().parents[2] / "config" / "client_map.json"
)


class ClientMapError(ValueError):
    """The table is malformed.

    Carries *every* problem found rather than the first, because these are
    hand-edited files and fixing typos one exception at a time is how a
    five-minute edit becomes a twenty-minute one.
    """


@dataclasses.dataclass(frozen=True)
class ClientEntry:
    """One client's resolved identity: who they are and what to pull."""

    token: str
    agency: str
    search_ids: tuple[str, ...]
    folder: str | None = None
    # Scheduling settings, edited through the brand app (CFG_PULSAR_CLIENT).
    # All optional so a hand-written map stays as short as it always was.
    active: bool = True
    window_days: int | None = None
    statuses: tuple[str, ...] | None = None
    backfill_requested_at: str | None = None


def parse_client_map(doc: Any) -> dict[str, ClientEntry]:
    """Validate a decoded client map and return it keyed by token.

    Pure, so the Lambda (reading S3) and the tools (reading a file) share
    one definition of what "valid" means instead of drifting apart.
    """
    problems: list[str] = []

    if not isinstance(doc, dict):
        raise ClientMapError(f"map must be a JSON object, got {type(doc).__name__}")
    clients = doc.get("clients")
    if not isinstance(clients, dict):
        raise ClientMapError("map must have a 'clients' object at the top level")
    if not clients:
        raise ClientMapError("map has no clients")

    entries: dict[str, ClientEntry] = {}
    # search_id / casefolded folder -> the token that already claimed it.
    claimed: dict[str, str] = {}
    claimed_folders: dict[str, str] = {}

    for token, raw in sorted(clients.items()):
        where = f"clients.{token}"
        if not CLIENT_TOKEN.match(token):
            problems.append(f"{where}: token must match {CLIENT_TOKEN.pattern}")
            continue
        if not isinstance(raw, dict):
            problems.append(f"{where}: entry must be an object")
            continue

        agency = raw.get("agency")
        if agency not in AGENCIES:
            problems.append(f"{where}.agency: expected one of {sorted(AGENCIES)}")

        folder = raw.get("folder")
        if folder is not None and (not isinstance(folder, str) or not folder.strip()):
            problems.append(f"{where}.folder: expected a non-empty string")
            folder = None
        elif isinstance(folder, str):
            folder = folder.strip()
            # Same wall check as search ids, one level up: two clients
            # resolving the same folder pull the same searches.
            owner = claimed_folders.get(folder.casefold())
            if owner is not None:
                problems.append(
                    f"{where}.folder: {folder!r} is already claimed by "
                    f"clients.{owner}; a folder belongs to one client"
                )
                folder = None
            else:
                claimed_folders[folder.casefold()] = token

        # With a folder, search_ids are optional pins (searches that live
        # outside the folder). Without one they are the whole scope.
        raw_ids = raw.get("search_ids", [] if folder else None)
        if not isinstance(raw_ids, list) or (not raw_ids and not folder):
            problems.append(
                f"{where}.search_ids: expected a non-empty list (or set 'folder')"
            )
            continue

        ids: list[str] = []
        for value in raw_ids:
            # Accept ints so the file can be written the way the API prints
            # them, but normalise to str: the id becomes a path segment.
            sid = str(value).strip()
            if not sid.isdigit():
                problems.append(f"{where}.search_ids: {value!r} is not a numeric id")
                continue
            if sid in ids:
                problems.append(f"{where}.search_ids: {sid} listed twice")
                continue
            # The check that matters. Two clients sharing a search id means
            # one brand's mentions land under the other brand's token - a
            # wall breach that looks like a working export.
            owner = claimed.get(sid)
            if owner is not None:
                problems.append(
                    f"{where}.search_ids: {sid} is already claimed by "
                    f"clients.{owner}; a search belongs to one client"
                )
                continue
            claimed[sid] = token
            ids.append(sid)

        settings = _settings(where, raw, problems)

        if (ids or folder) and agency in AGENCIES:
            entries[token] = ClientEntry(
                token=token,
                agency=str(agency),
                search_ids=tuple(ids),
                folder=folder,
                **settings,
            )

    if problems:
        raise ClientMapError(
            f"{len(problems)} problem(s) in the client map:\n  " + "\n  ".join(problems)
        )
    return entries


MAX_WINDOW_DAYS = 3650


def _settings(where: str, raw: dict[str, Any], problems: list[str]) -> dict[str, Any]:
    """The optional scheduling fields of one entry, validated.

    An inactive entry is still validated in full and still claims its
    folder and searches: switching a brand off must not let another brand
    quietly take over its data.
    """
    out: dict[str, Any] = {}
    active = raw.get("active", True)
    if not isinstance(active, bool):
        problems.append(f"{where}.active: expected true or false")
    else:
        out["active"] = active

    window = raw.get("window_days")
    if window is not None:
        if (
            isinstance(window, bool)
            or not isinstance(window, int)
            or not 1 <= window <= MAX_WINDOW_DAYS
        ):
            problems.append(
                f"{where}.window_days: expected a whole number 1-{MAX_WINDOW_DAYS}"
            )
        else:
            out["window_days"] = window

    statuses = raw.get("statuses")
    if statuses is not None:
        if not isinstance(statuses, list) or not all(
            isinstance(v, str) and v.strip() for v in statuses
        ):
            problems.append(f"{where}.statuses: expected a list of status names")
        elif statuses:
            out["statuses"] = tuple(v.strip().upper() for v in statuses)

    backfill = raw.get("backfill_requested_at")
    if backfill is not None:
        if not isinstance(backfill, str) or not backfill.strip():
            problems.append(f"{where}.backfill_requested_at: expected an ISO timestamp")
        else:
            out["backfill_requested_at"] = backfill.strip()
    return out


def load_client_map(path: str | pathlib.Path | None = None) -> dict[str, ClientEntry]:
    """Read and validate the table from disk."""
    path = pathlib.Path(path) if path is not None else DEFAULT_MAP_PATH
    if not path.exists():
        raise ClientMapError(
            f"no client map at {path}. Add the client there, or pass an "
            "explicit --search-id/--agency to bypass the map."
        )
    try:
        doc = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ClientMapError(f"{path} is not valid JSON: {exc}") from exc
    return parse_client_map(doc)


def resolve_targets(
    client: str,
    *,
    search_id: str | None = None,
    agency: str | None = None,
    map_path: str | pathlib.Path | None = None,
) -> tuple[str, list[str], str]:
    """Which searches a run covers, and under whose agency.

    Returns (agency, search_ids, provenance). Provenance is reported so a
    run's output says where its ids came from - the difference between "the
    table says so" and "someone typed it" is the difference between a
    reproducible export and a one-off.

    Passing BOTH `search_id` and `agency` bypasses the table entirely; that
    is the escape hatch for a search nobody has mapped yet. Anything less
    consults it.
    """
    if search_id and agency:
        return agency, [str(search_id)], "--search-id/--agency override"

    entry = resolve_client(load_client_map(map_path), client)
    return (
        agency or entry.agency,
        [str(search_id)] if search_id else list(entry.search_ids),
        str(map_path or "config/client_map.json"),
    )


@dataclasses.dataclass(frozen=True)
class Scope:
    """What one pull covers: an agency, and a folder and/or pinned ids."""

    agency: str
    folder: str | None
    search_ids: tuple[str, ...]
    only_search: str | None
    provenance: str


def resolve_scope(
    client: str,
    *,
    folder: str | None = None,
    search_id: str | None = None,
    agency: str | None = None,
    entries: dict[str, ClientEntry] | None = None,
    map_path: str | pathlib.Path | None = None,
) -> Scope:
    """Which folder/searches a pull covers, and under whose agency.

    Precedence, most explicit first:

      --folder or --search-id WITH --agency   bypasses the map entirely -
                                              the escape hatch for a brand
                                              nobody has mapped yet
      otherwise                               the map entry, with any of
                                              folder / agency overridden

    `search_id` always narrows the run to that one search (it is also
    pinned, so it is found even when it sits outside the folder).
    `entries` lets the Lambda pass a map it read from S3.
    """
    only = str(search_id) if search_id else None
    if (folder or search_id) and agency:
        if agency not in AGENCIES:
            raise ClientMapError(f"agency must be one of {sorted(AGENCIES)}")
        return Scope(agency, folder, (), only, "command line override")

    table = entries if entries is not None else load_client_map(map_path)
    entry = resolve_client(table, client)
    chosen = agency or entry.agency
    if chosen not in AGENCIES:
        raise ClientMapError(f"agency must be one of {sorted(AGENCIES)}")
    return Scope(
        chosen,
        folder or entry.folder,
        entry.search_ids,
        only,
        "client map"
        if entries is not None
        else str(map_path or "config/client_map.json"),
    )


def resolve_client(entries: dict[str, ClientEntry], token: str) -> ClientEntry:
    """Look up one client, failing with the list of what *is* mapped.

    An unmapped client is the common case today - 2 of 88 searches are
    mapped - so the error names the alternatives instead of just saying no.
    """
    entry = entries.get(token)
    if entry is None:
        known = ", ".join(sorted(entries)) or "(none)"
        raise ClientMapError(f"client {token!r} is not in the map. Mapped: {known}")
    return entry
