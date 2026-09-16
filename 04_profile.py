"""Step 4 - profile what we pulled, and answer the client's question once.

Two jobs:

  1. Data quality reality check. Which fields are actually populated,
     per source? This is where we find out whether the "NULL is not zero"
     problem (study §6.1) is real and how bad it is.

  2. Prove the data answers the question. Daily mention volume is the
     thing the client actually wants to see. If this chart is useful,
     the build is justified. If it is not, we have saved three weeks.

Usage: python 04_profile.py
Output: out/profile.txt, out/daily_volume.png
"""

import json
import pathlib

import pandas as pd

OUT = pathlib.Path("out")
SRC = OUT / "mentions.jsonl"

METRICS = [
    "likesCount", "sharesCount", "commentsCount",
    "engagement", "impressions", "potentialReach",
]


def load() -> pd.DataFrame:
    if not SRC.exists():
        raise SystemExit(f"{SRC} not found - run 03_extract.py first.")
    rows = [json.loads(line) for line in SRC.open()]
    if not rows:
        raise SystemExit("No rows extracted. Widen the date window and retry.")
    return pd.DataFrame(rows)


def main() -> int:
    df = load()
    report: list[str] = []

    def say(line: str = "") -> None:
        print(line)
        report.append(line)

    say(f"rows: {len(df):,}")
    say(f"columns: {', '.join(df.columns)}")
    say()

    # --- coverage: the finding that shapes the warehouse model -----------
    say("FIELD COVERAGE (% populated)")
    say("-" * 60)
    coverage = (df.notna().mean() * 100).sort_values(ascending=False)
    for col, pct in coverage.items():
        flag = "  <-- sparse" if pct < 95 else ""
        say(f"  {col:<26} {pct:6.1f}%{flag}")
    say()

    if "source" in df.columns:
        present = [m for m in METRICS if m in df.columns]
        if present:
            say("METRIC AVAILABILITY BY SOURCE (% populated)")
            say("-" * 60)
            by_source = df.groupby("source")[present].apply(
                lambda g: g.notna().mean() * 100
            )
            say(by_source.round(1).to_string())
            say()
            say("  Any column that is 0% for a source must be NULL in the")
            say("  warehouse, never 0 - averaging it across sources would")
            say("  otherwise be quietly wrong.")
            say()

        say("VOLUME BY SOURCE")
        say("-" * 60)
        say(df["source"].value_counts().to_string())
        say()

    # --- the chart the client actually cares about -----------------------
    if "publishedAt" in df.columns:
        df["published"] = pd.to_datetime(
            df["publishedAt"], errors="coerce", utc=True
        )
        bad = df["published"].isna().sum()
        if bad:
            say(f"WARNING: {bad} rows have unparseable publishedAt")

        daily = (
            df.dropna(subset=["published"])
              .set_index("published")
              .resample("D")
              .size()
        )

        say("DAILY MENTION VOLUME")
        say("-" * 60)
        say(daily.to_string())
        say()
        say(f"  range: {daily.index.min().date()} -> {daily.index.max().date()}")
        say(f"  peak:  {daily.max():,} on {daily.idxmax().date()}")
        say(f"  mean:  {daily.mean():.0f}/day")
        say()

        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            fig, ax = plt.subplots(figsize=(11, 4))
            ax.fill_between(daily.index, daily.values, alpha=0.25, color="#14615C")
            ax.plot(daily.index, daily.values, color="#14615C", linewidth=1.6)
            ax.set_title("Daily mention volume", loc="left", fontsize=12)
            ax.set_ylabel("mentions")
            ax.grid(axis="y", alpha=0.25)
            ax.spines[["top", "right"]].set_visible(False)
            fig.tight_layout()
            fig.savefig(OUT / "daily_volume.png", dpi=150)
            say(f"chart -> {OUT / 'daily_volume.png'}")
        except ImportError:
            say("(matplotlib not installed - skipped chart)")

    # --- mutability probe -------------------------------------------------
    say()
    say("NEXT: re-run 03_extract.py over the same window tomorrow, then")
    say("diff engagement on identical posts. That measures how fast metrics")
    say("move and tells us the right snapshot cadence (study §6.3).")

    (OUT / "profile.txt").write_text("\n".join(report))
    print(f"\nreport -> {OUT / 'profile.txt'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
