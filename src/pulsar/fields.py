"""Field lists for TracContent, generated from the introspected schema.

Hand-maintaining ~120 field names is how a vendor schema change turns into
a silent data gap. This reads schema/trac_data.json instead, so a re-run of
tools/introspect.py is the whole upgrade path.
"""

import json
import pathlib
from typing import Any

# src/pulsar/fields.py -> repo root is three levels up. The vendor dump is
# gitignored and regenerable via `make introspect`.
SCHEMA = pathlib.Path(__file__).resolve().parents[2] / "schema" / "trac_data.json"

# Object-typed fields need a subselection, so they cannot go in a flat list.
# `report: Report` is the only one on TracContent today; it is requested
# explicitly with its four subfields.
REPORT_SUBSELECTION = "report { countryCode countryName marketArea topics }"

# Third-party personal data. These land in raw - redacting at extract is a
# one-way door - but downstream views gate them until Legal/InfoSec sign off.
PII_FIELDS = frozenset(
    {
        "userBio",
        "userGender",
        "userAge",
        "userLatitude",
        "userLongitude",
        "userAvatarUrl",
        "userScreenName",
        "userIdentifier",
        "userCity",
        "userCountryCode",
        "userInfo",
        "userName",
        "userRealName",
    }
)


JsonDict = dict[str, Any]


def _unwrap(type_ref: JsonDict) -> JsonDict:
    """Strip NON_NULL / LIST wrappers down to the underlying named type.

    GraphQL nests these, so `[String!]!` arrives three levels deep.
    """
    while type_ref.get("ofType"):
        type_ref = type_ref["ofType"]
    return type_ref


def load_trac_content_fields(
    schema_path: pathlib.Path | None = None,
) -> tuple[list[str], list[str]]:
    """Return (flat_scalar_fields, object_fields) for TracContent.

    The path is resolved at call time, not import time, so callers and
    tests can point this at a different dump.
    """
    schema_path = schema_path or SCHEMA

    if not schema_path.exists():
        raise FileNotFoundError(
            f"{schema_path} missing - run tools/introspect.py first."
        )

    doc = json.loads(schema_path.read_text())
    types = {t["name"]: t for t in doc["__schema"]["types"]}

    trac_content = types.get("TracContent")
    if not trac_content:
        raise KeyError("TracContent not found in schema - did introspection change?")

    flat, objects = [], []
    for field in trac_content["fields"] or []:
        kind = _unwrap(field["type"])["kind"]
        if kind in ("SCALAR", "ENUM"):
            flat.append(field["name"])
        else:
            objects.append(field["name"])

    return sorted(flat), sorted(objects)


def pii_present(flat: list[str]) -> list[str]:
    """The PII policy names that actually exist in the schema.

    PII_FIELDS is a policy list, so it may name a field the vendor has since
    removed or not yet shipped. Counts and reporting must use the
    intersection, not the policy list's length.
    """
    return sorted(set(flat) & PII_FIELDS)


def pii_not_in_schema(schema_path: pathlib.Path | None = None) -> list[str]:
    """Policy names with no matching schema field.

    A name here is not itself a leak - excluding a field that does not exist
    is harmless. It is a staleness signal: the list has drifted from the
    vendor's schema, and the case that actually leaks is a field RENAMED
    rather than removed, which looks identical from here. A test asserts this
    is empty so a schema regeneration forces someone to look.
    """
    flat, _ = load_trac_content_fields(schema_path)
    return sorted(PII_FIELDS - set(flat))


def requested_fields(
    include_pii: bool = True, schema_path: pathlib.Path | None = None
) -> list[str]:
    """Exactly the flat fields a selection will ask for.

    Callers report counts from this rather than computing
    `len(flat) - len(PII_FIELDS)`, which over-counts whenever the policy list
    names a field the schema does not have.
    """
    flat, _ = load_trac_content_fields(schema_path)
    if not include_pii:
        flat = [f for f in flat if f not in PII_FIELDS]
    return flat


def results_selection(
    include_pii: bool = True, schema_path: pathlib.Path | None = None
) -> str:
    """The selection set for a `results` query covering every field."""
    flat, objects = load_trac_content_fields(schema_path)

    if not include_pii:
        flat = [f for f in flat if f not in PII_FIELDS]

    selection = list(flat)

    # Only `report` is expected; anything else new means the schema grew an
    # object field we have not written a subselection for.
    unexpected = [o for o in objects if o != "report"]
    if unexpected:
        raise ValueError(
            f"New object-typed fields on TracContent need subselections: {unexpected}"
        )
    if "report" in objects:
        selection.append(REPORT_SUBSELECTION)

    return "\n        ".join(selection)


if __name__ == "__main__":
    flat, objects = load_trac_content_fields()
    print(f"{len(flat)} flat fields, {len(objects)} object fields {objects}")
    print(f"{len(pii_present(flat))} of the flat fields are classified PII")
    stale = sorted(PII_FIELDS - set(flat))
    if stale:
        print(f"WARNING: PII policy names absent from the schema: {stale}")
