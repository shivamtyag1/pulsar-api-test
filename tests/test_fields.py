"""Field-list generation from the introspected schema.

`schema/` is gitignored - it is a large, regenerable vendor artifact - so
these tests build their own synthetic schema rather than depending on it.
The one test that reads the real dump is skipped when it is absent.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from pulsar import fields
from pulsar.fields import (
    PII_FIELDS,
    load_trac_content_fields,
    pii_not_in_schema,
    pii_present,
    requested_fields,
    results_selection,
)


def scalar(name: str) -> dict[str, object]:
    return {"name": name, "type": {"kind": "SCALAR", "name": "String", "ofType": None}}


def non_null_scalar(name: str) -> dict[str, object]:
    """A `String!` - the wrapper mypy-style unwrapping must see through."""
    return {
        "name": name,
        "type": {
            "kind": "NON_NULL",
            "name": None,
            "ofType": {"kind": "SCALAR", "name": "String", "ofType": None},
        },
    }


def enum_field(name: str) -> dict[str, object]:
    return {
        "name": name,
        "type": {"kind": "ENUM", "name": "StatusEnum", "ofType": None},
    }


def object_field(name: str, type_name: str) -> dict[str, object]:
    return {"name": name, "type": {"kind": "OBJECT", "name": type_name, "ofType": None}}


def write_schema(
    tmp_path: pathlib.Path, trac_fields: list[dict[str, object]]
) -> pathlib.Path:
    doc = {
        "__schema": {
            "types": [
                {"name": "TracContent", "kind": "OBJECT", "fields": trac_fields},
                {"name": "Unrelated", "kind": "OBJECT", "fields": [scalar("noise")]},
            ]
        }
    }
    path = tmp_path / "trac_data.json"
    path.write_text(json.dumps(doc))
    return path


class TestFieldExtraction:
    def test_separates_scalars_from_objects(self, tmp_path: pathlib.Path) -> None:
        path = write_schema(
            tmp_path,
            [scalar("content"), enum_field("status"), object_field("report", "Report")],
        )

        flat, objects = load_trac_content_fields(path)

        assert flat == ["content", "status"]
        assert objects == ["report"]

    def test_unwraps_non_null_and_list_wrappers(self, tmp_path: pathlib.Path) -> None:
        """`String!` is still a scalar and must be requestable flat."""
        path = write_schema(tmp_path, [non_null_scalar("searchHash")])

        flat, objects = load_trac_content_fields(path)

        assert flat == ["searchHash"]
        assert objects == []

    def test_output_is_sorted_for_stable_diffs(self, tmp_path: pathlib.Path) -> None:
        path = write_schema(tmp_path, [scalar("zulu"), scalar("alpha"), scalar("mike")])

        flat, _ = load_trac_content_fields(path)

        assert flat == ["alpha", "mike", "zulu"]

    def test_missing_schema_file_is_actionable(self, tmp_path: pathlib.Path) -> None:
        with pytest.raises(FileNotFoundError, match="tools/introspect"):
            load_trac_content_fields(tmp_path / "absent.json")

    def test_missing_trac_content_type_is_actionable(
        self, tmp_path: pathlib.Path
    ) -> None:
        path = tmp_path / "s.json"
        path.write_text(json.dumps({"__schema": {"types": [{"name": "Other"}]}}))

        with pytest.raises(KeyError, match="TracContent"):
            load_trac_content_fields(path)


class TestSelectionBuilding:
    def test_includes_report_subselection(self, tmp_path: pathlib.Path) -> None:
        path = write_schema(
            tmp_path, [scalar("content"), object_field("report", "Report")]
        )

        selection = results_selection(schema_path=path)

        assert "content" in selection
        assert "report { countryCode countryName marketArea topics }" in selection

    def test_new_object_field_fails_loudly(self, tmp_path: pathlib.Path) -> None:
        """A vendor adding an object field must break the build, not the data.

        Requesting an object without a subselection is a GraphQL error, so
        failing here converts a mid-run extraction failure into a clear one.
        """
        path = write_schema(
            tmp_path,
            [scalar("content"), object_field("newThing", "SomeNewType")],
        )

        with pytest.raises(ValueError, match="newThing"):
            results_selection(schema_path=path)

    def test_pii_can_be_excluded(self, tmp_path: pathlib.Path) -> None:
        path = write_schema(
            tmp_path, [scalar("content"), scalar("userBio"), scalar("userGender")]
        )

        with_pii = results_selection(include_pii=True, schema_path=path)
        without_pii = results_selection(include_pii=False, schema_path=path)

        assert "userBio" in with_pii
        assert "userBio" not in without_pii
        assert "userGender" not in without_pii
        assert "content" in without_pii

    def test_default_schema_path_resolves_at_call_time(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Guards the bug this signature change fixed.

        The path used to be bound as a default argument at import time, so
        tests silently read the real vendor dump and passed for the wrong
        reason.
        """
        path = write_schema(tmp_path, [scalar("onlyInFixture")])
        monkeypatch.setattr(fields, "SCHEMA", path)

        flat, _ = load_trac_content_fields()

        assert flat == ["onlyInFixture"]


class TestPIIClassification:
    @pytest.mark.parametrize(
        "field_name",
        ["userBio", "userGender", "userAge", "userLatitude", "userLongitude"],
    )
    def test_sensitive_author_attributes_are_classified(self, field_name: str) -> None:
        """These are third-party personal data and gate the staging views."""
        assert field_name in PII_FIELDS

    @pytest.mark.parametrize("field_name", ["content", "source", "likesCount"])
    def test_ordinary_metrics_are_not_classified_as_pii(self, field_name: str) -> None:
        assert field_name not in PII_FIELDS


class TestAgainstTheRealSchema:
    """Guards the real vendor dump when it is present locally."""

    @pytest.fixture
    def real_schema(self) -> pathlib.Path:
        path = fields.SCHEMA
        if not path.exists():
            pytest.skip("schema/trac_data.json absent - run tools/introspect.py")
        return path

    def test_report_is_the_only_object_field(self, real_schema: pathlib.Path) -> None:
        _, objects = load_trac_content_fields(real_schema)
        assert objects == ["report"], (
            "the vendor added an object field; give it a subselection in fields.py"
        )

    def test_key_pipeline_fields_are_present(self, real_schema: pathlib.Path) -> None:
        """The incremental contract depends on all four of these."""
        flat, _ = load_trac_content_fields(real_schema)
        for required in ("pulsarId", "updatedAt", "publishedAt", "search"):
            assert required in flat, f"{required} missing - design assumes it exists"


class TestPiiPolicyAgainstSchema:
    """PII_FIELDS is a policy list; the schema is ground truth.

    The failure these guard against is silent: an export labelled "PII
    excluded" that contains personal data because a policy name stopped
    matching the schema.
    """

    def test_no_pii_name_survives_into_a_non_pii_selection(
        self, tmp_path: pathlib.Path
    ) -> None:
        path = write_schema(
            tmp_path, [scalar("userBio"), scalar("userAge"), scalar("content")]
        )
        sel = results_selection(include_pii=False, schema_path=path).split()
        assert "userBio" not in sel
        assert "userAge" not in sel
        assert "content" in sel

    def test_requested_count_matches_the_selection_exactly(
        self, tmp_path: pathlib.Path
    ) -> None:
        """Counting as len(flat) - len(PII_FIELDS) over-counts when the policy
        names a field the schema does not have - which it currently does."""
        path = write_schema(tmp_path, [scalar("userBio"), scalar("content")])
        got = requested_fields(include_pii=False, schema_path=path)
        sel = results_selection(include_pii=False, schema_path=path).split()
        assert got == ["content"]
        assert len([t for t in sel if t in got]) == len(got)

    def test_policy_names_absent_from_the_schema_are_reported(
        self, tmp_path: pathlib.Path
    ) -> None:
        path = write_schema(tmp_path, [scalar("content")])
        stale = pii_not_in_schema(path)
        assert "userBio" in stale  # declared in policy, absent from this schema

    def test_pii_present_is_the_intersection_not_the_policy(
        self, tmp_path: pathlib.Path
    ) -> None:
        path = write_schema(tmp_path, [scalar("userBio"), scalar("content")])
        flat, _ = load_trac_content_fields(path)
        assert pii_present(flat) == ["userBio"]
        assert len(pii_present(flat)) < len(PII_FIELDS)
