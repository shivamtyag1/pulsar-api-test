"""Search -> client attribution heuristics.

The cases below are real search names from the account (88 of them), not
invented examples. The Pepsi case is a genuine regression: an earlier
vowel-blind heuristic flagged the single largest search in the account -
5.9M mentions - as a test, which would have skewed the whole audit.
"""

from __future__ import annotations

import pathlib

import pytest

from pulsar.mapping import (
    ClientMapError,
    guess_client,
    is_extractable,
    load_client_map,
    looks_like_junk,
    parse_client_map,
    resolve_client,
    resolve_scope,
    resolve_targets,
)


class TestJunkDetection:
    @pytest.mark.parametrize(
        "name",
        [
            "Pepsi",  # 5.9M mentions - the regression case
            "Clif",
            "Lovesac",
            "Google Australia",
            "Greenworks Tools",
            "Arcteryx - General",
            "Maxis Malaysia",
            "Indeed 2026 Job Market Sentiment",
            "HnR Block",
        ],
    )
    def test_real_client_searches_are_not_flagged(self, name: str) -> None:
        assert looks_like_junk(name) is False, f"{name!r} is real client work"

    @pytest.mark.parametrize(
        "name",
        [
            "test",
            "general",
            "Test - Thailand Trending Convos ",  # 2.0M mentions
            "Google Australia - Test",  # 1.6M mentions
            "Google SEA - ID test",
            "Gusto Test",
            "2026 Aug - AU vetting - test v2",
            "VAYNER MEDIA TRAINING",
            "hjknlm",
            "bgtbrfvdcs",
        ],
    )
    def test_placeholder_and_test_searches_are_flagged(self, name: str) -> None:
        assert looks_like_junk(name) is True, f"{name!r} should be flagged"

    @pytest.mark.parametrize("name", [None, "", "   "])
    def test_empty_names_are_flagged(self, name: str | None) -> None:
        """9 of 88 searches have a null name; all are drafts."""
        assert looks_like_junk(name) is True

    def test_vowel_heuristic_does_not_fire_on_multiword_names(self) -> None:
        """Only single tokens are candidates for keyboard mash."""
        assert looks_like_junk("Thin Sliced Chicken Breast") is False


class TestClientGuess:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("Arcteryx - General", "Arcteryx"),
            ("Google JP - Casting Creteria", "Google JP"),
            ("Google SEA_(TH PH VN)", "Google SEA"),
            ("Tyson Chicken x Wingstop", "Tyson Chicken"),
            ("Pepsi", "Pepsi"),
            ("Lovesac", "Lovesac"),
        ],
    )
    def test_extracts_the_leading_client_token(self, name: str, expected: str) -> None:
        assert guess_client(name) == expected

    @pytest.mark.parametrize(
        "name",
        [
            None,
            "",
            # Purpose-only names carry no client at all - a guess here
            # would be fabrication.
            'Universal / Pan-Regional "Low-Cognitive" Tracker',
            "Education Alternatives Conversation",
        ],
    )
    def test_returns_blank_rather_than_guessing_wildly(self, name: str | None) -> None:
        assert guess_client(name) == ""

    def test_prefers_the_dominant_separator_over_the_proposed_one(self) -> None:
        """ " - " is what the account actually uses; "_" is the proposal."""
        assert guess_client("Nike - Summer_Campaign") == "Nike"

    def test_rejects_an_absurdly_long_head(self) -> None:
        long_head = "x" * 50
        assert guess_client(f"{long_head} - thing") == ""


class TestExtractionEligibility:
    def test_a_mapped_active_ready_search_with_data_is_extractable(self) -> None:
        assert is_extractable(
            mapped=True, active=True, total_contents=2770, status="READY"
        )

    @pytest.mark.parametrize("status", ["READY", "EDIT", "PREPARED"])
    def test_workable_statuses(self, status: str) -> None:
        assert is_extractable(
            mapped=True, active=True, total_contents=10, status=status
        )

    @pytest.mark.parametrize("status", ["DRAFT", "ARCHIVED", "DELETING", None])
    def test_unworkable_statuses_are_skipped(self, status: str | None) -> None:
        assert not is_extractable(
            mapped=True, active=True, total_contents=10, status=status
        )

    def test_unmapped_searches_are_never_extracted(self) -> None:
        """Quarantine is what makes the nomenclature ask enforceable."""
        assert not is_extractable(
            mapped=False, active=True, total_contents=999_999, status="READY"
        )

    def test_empty_searches_are_skipped(self) -> None:
        """44 of 88 searches have zero content; they cost calls for nothing."""
        assert not is_extractable(
            mapped=True, active=True, total_contents=0, status="READY"
        )

    def test_inactive_searches_are_skipped(self) -> None:
        assert not is_extractable(
            mapped=True, active=False, total_contents=100, status="READY"
        )


class TestClientMapValidation:
    """The table decides which client's name goes on which mentions, so a
    malformed entry is a misattribution risk rather than a typo. Every case
    here is something a human plausibly types into a hand-edited file."""

    @staticmethod
    def doc(**clients: object) -> dict[str, object]:
        return {"version": 1, "clients": clients}

    def test_a_good_map_parses(self) -> None:
        entries = parse_client_map(
            self.doc(
                **{
                    "raisin-bran": {"agency": "VM", "search_ids": ["176652"]},
                    "lovesac": {"agency": "VM", "search_ids": ["224320", "224321"]},
                }
            )
        )
        assert set(entries) == {"raisin-bran", "lovesac"}
        assert entries["lovesac"].search_ids == ("224320", "224321")
        assert entries["raisin-bran"].agency == "VM"

    def test_numeric_ids_are_normalised_to_strings(self) -> None:
        """The file may be written with the ints the API prints, but the id
        becomes a path segment, so it has to be a string by the time it is
        used - not `search=176652.0` or `search=176652 `."""
        entries = parse_client_map(
            self.doc(**{"acme": {"agency": "VM", "search_ids": [176652, " 224320 "]}})
        )
        assert entries["acme"].search_ids == ("176652", "224320")

    def test_two_clients_cannot_share_a_search(self) -> None:
        """The wall breach. One search feeding two client tokens means one
        brand's mentions land in the other brand's prefix, and the export
        looks entirely healthy while doing it."""
        with pytest.raises(ClientMapError, match="already claimed"):
            parse_client_map(
                self.doc(
                    **{
                        "brand-a": {"agency": "VM", "search_ids": ["176652"]},
                        "brand-b": {"agency": "CM", "search_ids": ["176652"]},
                    }
                )
            )

    def test_every_problem_is_reported_not_just_the_first(self) -> None:
        with pytest.raises(ClientMapError) as exc:
            parse_client_map(
                self.doc(
                    **{
                        "Bad Token": {"agency": "VM", "search_ids": ["1"]},
                        "ok-client": {"agency": "XX", "search_ids": ["not-a-number"]},
                    }
                )
            )
        message = str(exc.value)
        assert "Bad Token" in message
        assert "agency" in message
        assert "not-a-number" in message

    @pytest.mark.parametrize(
        ("entry", "expected"),
        [
            ({"agency": "VM"}, "search_ids"),
            ({"agency": "VM", "search_ids": []}, "search_ids"),
            ({"search_ids": ["1"]}, "agency"),
            ({"agency": "vm", "search_ids": ["1"]}, "agency"),
        ],
    )
    def test_incomplete_entries_are_rejected(
        self, entry: dict[str, object], expected: str
    ) -> None:
        with pytest.raises(ClientMapError, match=expected):
            parse_client_map(self.doc(**{"acme": entry}))

    def test_a_folder_makes_search_ids_optional(self) -> None:
        entries = parse_client_map(
            self.doc(**{"lovesac": {"agency": "VM", "folder": " Lovesac "}})
        )
        assert entries["lovesac"].folder == "Lovesac"
        assert entries["lovesac"].search_ids == ()

    def test_folder_plus_pins(self) -> None:
        entries = parse_client_map(
            self.doc(
                **{"lovesac": {"agency": "VM", "folder": "L", "search_ids": ["9"]}}
            )
        )
        assert entries["lovesac"].search_ids == ("9",)

    def test_a_folder_belongs_to_one_client(self) -> None:
        """Same wall check as search ids, one level up - case-insensitive,
        because the folder lookup is."""
        with pytest.raises(ClientMapError, match="already claimed"):
            parse_client_map(
                self.doc(
                    **{
                        "brand-a": {"agency": "VM", "folder": "Lovesac"},
                        "brand-b": {"agency": "CM", "folder": "lovesac"},
                    }
                )
            )

    @pytest.mark.parametrize("folder", ["", "  ", 7])
    def test_a_bad_folder_is_rejected(self, folder: object) -> None:
        with pytest.raises(ClientMapError, match="folder"):
            parse_client_map(self.doc(**{"acme": {"agency": "VM", "folder": folder}}))

    def test_an_empty_map_is_an_error_not_an_empty_result(self) -> None:
        """Returning {} would make every lookup fail with "not mapped",
        which reads like a missing client rather than a broken file."""
        with pytest.raises(ClientMapError, match="no clients"):
            parse_client_map({"clients": {}})

    def test_resolve_names_what_is_mapped(self) -> None:
        entries = parse_client_map(
            self.doc(**{"lovesac": {"agency": "VM", "search_ids": ["224320"]}})
        )
        assert resolve_client(entries, "lovesac").search_ids == ("224320",)
        with pytest.raises(ClientMapError, match="lovesac"):
            resolve_client(entries, "kotex")


def test_the_repo_map_is_valid() -> None:
    """The checked-in table is the thing that gets copied to S3 and read by
    the deployed function. A broken commit here breaks every export."""
    entries = load_client_map()
    assert entries, "config/client_map.json resolved to nothing"
    for token, entry in entries.items():
        assert entry.token == token
        assert entry.search_ids or entry.folder, f"{token} has no folder or ids"


class TestResolveTargets:
    """Both operator tools and the Lambda route through this, so it decides
    whose data a run pulls."""

    def test_a_mapped_client_needs_nothing_else(self) -> None:
        agency, ids, why = resolve_targets("raisin-bran")
        assert agency == "VM"
        assert ids == ["176652"]
        assert "client_map.json" in why

    def test_full_override_never_reads_the_map(self, tmp_path: pathlib.Path) -> None:
        """The unmapped-search escape hatch has to work when the map cannot
        be read at all, or it is not an escape hatch."""
        agency, ids, why = resolve_targets(
            "kotex",
            search_id="226781",
            agency="VM",
            map_path=tmp_path / "does-not-exist.json",
        )
        assert (agency, ids) == ("VM", ["226781"])
        assert "override" in why

    def test_agency_alone_overrides_but_ids_still_come_from_the_map(self) -> None:
        agency, ids, _ = resolve_targets("raisin-bran", agency="CM")
        assert agency == "CM"
        assert ids == ["176652"]

    def test_an_unmapped_client_without_overrides_fails(self) -> None:
        with pytest.raises(ClientMapError, match="not in the map"):
            resolve_targets("kotex")


class TestResolveScope:
    """What tools/pull.py and the Lambda's pull action route through."""

    def test_a_mapped_client(self) -> None:
        scope = resolve_scope("raisin-bran")
        assert scope.agency == "VM"
        assert scope.search_ids == ("176652",)
        assert scope.only_search is None
        assert "client_map.json" in scope.provenance

    def test_folder_override_keeps_the_mapped_agency(self) -> None:
        scope = resolve_scope("raisin-bran", folder="Raisin Bran")
        assert (scope.agency, scope.folder) == ("VM", "Raisin Bran")

    def test_folder_with_agency_never_reads_the_map(
        self, tmp_path: pathlib.Path
    ) -> None:
        scope = resolve_scope(
            "new-brand",
            folder="New Brand",
            agency="CM",
            map_path=tmp_path / "missing.json",
        )
        assert (scope.agency, scope.folder, scope.search_ids) == ("CM", "New Brand", ())
        assert "override" in scope.provenance

    def test_search_id_narrows(self) -> None:
        scope = resolve_scope("raisin-bran", search_id="176652")
        assert scope.only_search == "176652"

    def test_entries_passed_in(self) -> None:
        entries = parse_client_map(
            {"clients": {"lovesac": {"agency": "TG", "folder": "Lovesac"}}}
        )
        scope = resolve_scope("lovesac", entries=entries)
        assert (scope.agency, scope.folder) == ("TG", "Lovesac")
        assert scope.provenance == "client map"

    def test_bad_agency(self) -> None:
        with pytest.raises(ClientMapError, match="agency"):
            resolve_scope("x", folder="F", agency="ZZ")
        with pytest.raises(ClientMapError, match="agency"):
            resolve_scope("raisin-bran", agency="ZZ")


class TestSchedulingSettings:
    """The optional fields the brand app edits (CFG_PULSAR_CLIENT)."""

    def entry(self, **extra: object) -> dict[str, object]:
        return {"clients": {"perdue": {"agency": "VM", "folder": "920", **extra}}}

    def test_defaults(self) -> None:
        e = parse_client_map(self.entry())["perdue"]
        assert e.active is True and e.window_days is None
        assert e.statuses is None and e.backfill_requested_at is None

    def test_all_fields(self) -> None:
        e = parse_client_map(
            self.entry(
                active=False,
                window_days=14,
                statuses=["ready", " archived "],
                backfill_requested_at="2026-10-01T12:00:00Z",
            )
        )["perdue"]
        assert e.active is False and e.window_days == 14
        assert e.statuses == ("READY", "ARCHIVED")
        assert e.backfill_requested_at == "2026-10-01T12:00:00Z"

    def test_empty_status_list_means_default(self) -> None:
        assert parse_client_map(self.entry(statuses=[]))["perdue"].statuses is None

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("active", "yes"),
            ("window_days", 0),
            ("window_days", True),
            ("window_days", 99999),
            ("statuses", "READY"),
            ("statuses", [""]),
            ("backfill_requested_at", 5),
        ],
    )
    def test_bad_values_are_reported(self, field: str, value: object) -> None:
        with pytest.raises(ClientMapError, match=field):
            parse_client_map(self.entry(**{field: value}))

    def test_inactive_brand_still_claims_its_folder(self) -> None:
        doc = {
            "clients": {
                "a": {"agency": "VM", "folder": "920", "active": False},
                "b": {"agency": "CM", "folder": "920"},
            }
        }
        with pytest.raises(ClientMapError, match="already claimed"):
            parse_client_map(doc)
