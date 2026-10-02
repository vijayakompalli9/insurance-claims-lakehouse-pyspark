"""Seeded synthetic feed generator for Northwind Mutual Insurance (fictional).

Produces two daily batches of raw feeds under ``<raw_root>/batch_<n>/``:

* ``customers.csv``  - customer master (batch 2 carries new + updated customers)
* ``policies.json``  - newline-delimited JSON policy snapshots / changes
* ``claims.csv``     - claim events, including deliberately bad rows

Every value is random but reproducible from a fixed seed. No real people,
companies or schemas are represented.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

CUSTOMER_COLS = ["customer_id", "first_name", "last_name", "date_of_birth", "email",
                 "state", "segment", "updated_at"]
CLAIM_COLS = ["claim_id", "policy_id", "loss_date", "reported_date", "claim_amount",
              "paid_amount", "claim_status", "cause_of_loss", "last_modified"]

FIRST = ["Avery", "Jordan", "Riley", "Casey", "Morgan", "Quinn", "Harper", "Rowan",
         "Emerson", "Skyler", "Dakota", "Reese", "Sage", "Parker", "Kendall", "Blake"]
LAST = ["Alder", "Birch", "Cedar", "Dunmore", "Ellery", "Fairfax", "Glenwood", "Hollis",
        "Ingram", "Juniper", "Kestrel", "Linden", "Marlow", "Norcross", "Oakley", "Prescott"]
STATES = ["CA", "TX", "NY", "FL", "IL", "WA", "GA", "OH", "AZ", "CO"]
SEGMENTS = ["RETAIL", "PREFERRED", "SMALL_BUSINESS"]
PRODUCTS = {"AUTO": (900, 2400), "HOME": (700, 2000), "LIFE": (300, 1500),
            "COMMERCIAL": (3000, 12000)}
CAUSES = {"AUTO": ["COLLISION", "THEFT", "GLASS"], "HOME": ["WATER", "FIRE", "WIND"],
          "LIFE": ["DEATH_BENEFIT", "ACCIDENTAL_DEATH"],
          "COMMERCIAL": ["LIABILITY", "PROPERTY", "BUSINESS_INTERRUPTION"]}
BATCH_WINDOWS = {1: (date(2026, 1, 1), date(2026, 1, 31)),
                 2: (date(2026, 2, 1), date(2026, 2, 28))}


def _ts(d: date, rng: random.Random) -> str:
    return datetime(d.year, d.month, d.day, rng.randint(0, 23), rng.randint(0, 59),
                    rng.randint(0, 59)).isoformat(sep=" ")


def _rand_date(rng: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=rng.randint(0, (end - start).days))


@dataclass
class _State:
    rng: random.Random
    customers: dict[str, dict] = field(default_factory=dict)
    policies: dict[str, dict] = field(default_factory=dict)
    claims: dict[str, dict] = field(default_factory=dict)
    next_cust: int = 1
    next_pol: int = 1
    next_claim: int = 1


def _new_customer(st: _State, updated: date) -> dict:
    rng = st.rng
    cid = f"C{st.next_cust:05d}"
    st.next_cust += 1
    first, last = rng.choice(FIRST), rng.choice(LAST)
    row = {"customer_id": cid, "first_name": first, "last_name": last,
           "date_of_birth": _rand_date(rng, date(1950, 1, 1), date(2003, 12, 31)).isoformat(),
           "email": f"{first.lower()}.{last.lower()}{cid[-3:]}@example.test",
           "state": rng.choice(STATES), "segment": rng.choice(SEGMENTS),
           "updated_at": _ts(updated, rng)}
    st.customers[cid] = row
    return row


def _new_policy(st: _State, customer_id: str, window: tuple[date, date]) -> dict:
    """Create a policy whose ``updated_at`` falls inside the batch ``window``."""
    rng = st.rng
    pid = f"P{st.next_pol:06d}"
    st.next_pol += 1
    product = rng.choice(list(PRODUCTS))
    lo, hi = PRODUCTS[product]
    start = _rand_date(rng, date(2025, 1, 1), date(2025, 12, 31))
    row = {"policy_id": pid, "customer_id": customer_id, "product_line": product,
           "premium_annual": round(rng.uniform(lo, hi), 2),
           "coverage_limit": rng.choice([50000, 100000, 250000, 500000, 1000000]),
           "deductible": rng.choice([250, 500, 1000, 2500]), "status": "ACTIVE",
           "policy_start": start.isoformat(),
           "policy_end": (start + timedelta(days=365)).isoformat(),
           "region": rng.choice(["WEST", "SOUTH", "NORTHEAST", "MIDWEST"]),
           "updated_at": _ts(_rand_date(rng, *window), rng)}
    st.policies[pid] = row
    return row


def _claim(st: _State, policy: dict, window: tuple[date, date]) -> dict:
    rng = st.rng
    cid = f"CLM{st.next_claim:07d}"
    st.next_claim += 1
    loss = _rand_date(rng, max(window[0], date.fromisoformat(policy["policy_start"])), window[1])
    reported = min(loss + timedelta(days=rng.randint(0, 5)), window[1])
    amount = round(rng.lognormvariate(7.4, 0.9), 2)
    if policy["product_line"] == "LIFE":
        amount = float(rng.choice([10000, 25000, 50000]))
    elif policy["product_line"] == "COMMERCIAL":
        amount = round(amount * 3, 2)
    status = rng.choice(["OPEN", "OPEN", "CLOSED", "DENIED"])
    paid = round(amount * rng.uniform(0.3, 1.0), 2) if status == "CLOSED" else 0.0
    row = {"claim_id": cid, "policy_id": policy["policy_id"], "loss_date": loss.isoformat(),
           "reported_date": reported.isoformat(), "claim_amount": f"{amount:.2f}",
           "paid_amount": f"{paid:.2f}", "claim_status": status,
           "cause_of_loss": rng.choice(CAUSES[policy["product_line"]]),
           "last_modified": _ts(reported, rng)}
    st.claims[cid] = row
    return row


def _bad_claims(st: _State, good: list[dict], batch: int) -> list[list[str]]:
    """Return deliberately defective claim rows (as raw CSV cells)."""
    rng = st.rng
    as_cells = lambda r: [str(r[c]) for c in CLAIM_COLS]  # noqa: E731
    base = rng.sample(good, 12)
    bad: list[list[str]] = []
    # null keys
    for r in base[0:3]:
        bad.append(as_cells({**r, "claim_id": ""}))
    # negative amounts (new ids so they are not also duplicates)
    for i, r in enumerate(base[3:6]):
        bad.append(as_cells({**r, "claim_id": f"CLMNEG{batch}{i:02d}",
                             "claim_amount": f"-{r['claim_amount']}"}))
    # unknown policy references
    for i, r in enumerate(base[6:9]):
        bad.append(as_cells({**r, "claim_id": f"CLMUNK{batch}{i:02d}",
                             "policy_id": f"P9{batch}{i:04d}"}))
    # bad dates: unparseable, impossible calendar date, reported before loss
    r = base[9]
    bad.append(as_cells({**r, "claim_id": f"CLMBDT{batch}00", "loss_date": "2026-13-45"}))
    bad.append(as_cells({**r, "claim_id": f"CLMBDT{batch}01", "loss_date": "31/01/2026"}))
    bad.append(as_cells({**r, "claim_id": f"CLMBDT{batch}02", "loss_date": r["reported_date"],
                         "reported_date": "2025-06-01"}))
    # non-numeric amount
    bad.append(as_cells({**base[10], "claim_id": f"CLMNAN{batch}00", "claim_amount": "12,5OO"}))
    # duplicate claim ids: an exact resend and an older superseded version
    dup1, dup2 = base[10], base[11]
    bad.append(as_cells(dup1))
    older = datetime.fromisoformat(dup2["last_modified"]) - timedelta(hours=6)
    bad.append(as_cells({**dup2, "claim_status": "OPEN", "paid_amount": "0.00",
                         "last_modified": older.isoformat(sep=" ")}))
    # structurally malformed row (too many columns)
    bad.append(as_cells(base[0]) + ["EXTRA", "COLUMNS"])
    return bad


def generate(raw_root: Path, seed: int = 42, n_customers: int = 1000,
             claims_per_batch: int = 80) -> dict[int, dict[str, int]]:
    """Write both batches; return per-batch row counts (including bad rows)."""
    st = _State(rng=random.Random(seed))
    counts: dict[int, dict[str, int]] = {}
    for batch in (1, 2):
        rng = st.rng
        w_start, w_end = BATCH_WINDOWS[batch]
        out = raw_root / f"batch_{batch}"
        out.mkdir(parents=True, exist_ok=True)

        # ---------------- customers ----------------
        if batch == 1:
            cust_rows = [_new_customer(st, w_end) for _ in range(n_customers)]
        else:
            changed = rng.sample(sorted(st.customers), 10)
            for cid in changed:
                st.customers[cid] = {**st.customers[cid], "state": rng.choice(STATES),
                                     "segment": rng.choice(SEGMENTS),
                                     "updated_at": _ts(_rand_date(rng, w_start, w_end), rng)}
            cust_rows = [st.customers[c] for c in changed]
            cust_rows += [_new_customer(st, w_end) for _ in range(15)]
        cust_cells = [[str(r[c]) for c in CUSTOMER_COLS] for r in cust_rows]
        cust_cells.append(["", "Nul", "Key", "1990-01-01", "x@example.test", "CA", "RETAIL",
                           _ts(w_end, rng)])
        with (out / "customers.csv").open("w", newline="") as fh:
            wr = csv.writer(fh)
            wr.writerow(CUSTOMER_COLS)
            wr.writerows(cust_cells)

        # ---------------- policies ----------------
        lines: list[str] = []
        if batch == 1:
            for cid in sorted(st.customers):
                for _ in range(rng.choice([1, 1, 2])):
                    lines.append(json.dumps(_new_policy(st, cid, (date(2025, 12, 1), w_end))))
        else:
            existing = sorted(st.policies)
            picks = rng.sample(existing, 60)
            changes, stale, touched = picks[:40], picks[40:55], picks[55:]
            for pid in changes:
                p = dict(st.policies[pid])
                kind = rng.choice(["premium", "deductible", "status"])
                if kind == "premium":
                    p["premium_annual"] = round(p["premium_annual"] * rng.uniform(1.03, 1.15), 2)
                elif kind == "deductible":
                    p["deductible"] = rng.choice([d for d in (250, 500, 1000, 2500)
                                                  if d != p["deductible"]])
                else:
                    p["status"] = "CANCELLED"
                p["updated_at"] = _ts(_rand_date(rng, w_start, date(2026, 2, 20)), rng)
                st.policies[pid] = p
                lines.append(json.dumps(p))
            # one policy changes twice inside the same batch
            twice = dict(st.policies[changes[0]])
            twice["coverage_limit"] = twice["coverage_limit"] * 2
            twice["updated_at"] = _ts(date(2026, 2, 25), rng)
            st.policies[twice["policy_id"]] = twice
            lines.append(json.dumps(twice))
            # stale resends (older than watermark) - must be ignored
            lines += [json.dumps(st.policies[pid]) for pid in stale]
            # touched but unchanged attributes - hash-diff must suppress a new version
            for pid in touched:
                p = {**st.policies[pid], "updated_at": _ts(_rand_date(rng, w_start, w_end), rng)}
                st.policies[pid] = p
                lines.append(json.dumps(p))
            new_cust = sorted(st.customers)[-15:]
            lines += [json.dumps(_new_policy(st, c, (w_start, w_end))) for c in new_cust]
        lines.append(json.dumps({**json.loads(lines[1]), "policy_id": None}))
        lines.append('{"policy_id": "P_BROKEN", "premium_annual": 12')  # malformed JSON
        (out / "policies.json").write_text("\n".join(lines) + "\n")

        # ---------------- claims ----------------
        active = [p for p in st.policies.values()
                  if p["status"] == "ACTIVE" and p["policy_end"] > w_end.isoformat()]
        weights = [0.15 if p["product_line"] == "LIFE" else 1.0 for p in active]
        good = [_claim(st, p, (w_start, w_end))
                for p in rng.choices(active, weights=weights, k=claims_per_batch)]
        if batch == 2:  # lifecycle updates to claims first seen in batch 1
            for cid in rng.sample([c for c in st.claims if c < good[0]["claim_id"]], 10):
                old = st.claims[cid]
                amt = float(old["claim_amount"])
                upd = {**old, "claim_status": "CLOSED",
                       "paid_amount": f"{amt * 0.8:.2f}",
                       "last_modified": _ts(_rand_date(rng, w_start, w_end), rng)}
                st.claims[cid] = upd
                good.append(upd)
        rows = [[str(r[c]) for c in CLAIM_COLS] for r in good] + _bad_claims(st, good, batch)
        rng.shuffle(rows)
        with (out / "claims.csv").open("w", newline="") as fh:
            wr = csv.writer(fh)
            wr.writerow(CLAIM_COLS)
            wr.writerows(rows)
        counts[batch] = {"customers": len(cust_cells), "policies": len(lines),
                         "claims": len(rows)}
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic insurance feeds")
    parser.add_argument("--out", default="data/raw", help="raw landing root")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    counts = generate(Path(args.out), seed=args.seed)
    for batch, c in counts.items():
        print(f"batch={batch} " + " ".join(f"{k}={v}" for k, v in c.items()))


if __name__ == "__main__":
    main()
