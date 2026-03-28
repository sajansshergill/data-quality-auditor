"""
generate_data.py
Generates a synthetic Salesforce-style CRM contact export with seeded dirty data.

Usage:
    python src/generate_data.py --records 5200 --out data/raw_contacts.csv
"""

import argparse
import random
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

fake = Faker("en_US")
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# ── Constants ──────────────────────────────────────────────────────────────────

ACCOUNT_TYPES = ["Enterprise", "SMB", "Startup", "Non-profit", "Government"]
INDUSTRIES = ["Healthcare", "Technology", "Finance", "Education", "Retail", "Manufacturing", "Real Estate"]
SEGMENTS = ["Enterprise", "SMB", "Startup", "Non-profit", "Government"]
US_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY",
]
INVALID_STATES = ["XX", "ZZ", "QQ", "PP", "AA", "BB"]


# ── Clean record generator ─────────────────────────────────────────────────────

def make_clean_record(record_id: int) -> dict:
    first = fake.first_name()
    last  = fake.last_name()
    domain = fake.domain_name()
    account_type = random.choice(ACCOUNT_TYPES)
    last_activity = fake.date_between(start_date="-18m", end_date="today")

    return {
        "Id":                str(record_id).zfill(6),
        "First_Name":        first,
        "Last_Name":         last,
        "Full_Name":         f"{first} {last}",
        "Email":             f"{first.lower()}.{last.lower()}@{domain}",
        "Phone":             fake.numerify("(###) ###-####"),
        "Account_Type":      account_type,
        "Industry":          random.choice(INDUSTRIES),
        "State":             random.choice(US_STATES),
        "Company":           fake.company(),
        "Email_Domain":      domain,
        "Segment":           account_type,          # matches Account_Type for clean records
        "Last_Activity_Date": str(last_activity),
        "Created_Date":      str(fake.date_between(start_date="-5y", end_date="-18m")),
        "is_dirty":          False,
        "dirty_reason":      "",
    }


# ── Dirty data injectors ───────────────────────────────────────────────────────

def inject_duplicates(records: list, n: int = 127) -> list:
    """Clone n existing records with slight name/email variations."""
    indices = random.sample(range(len(records)), n)
    dupes = []
    for idx in indices:
        orig = records[idx].copy()
        # Vary the name slightly: swap char, add space, common nickname
        name_variants = [
            orig["Full_Name"],
            orig["Full_Name"].replace("a", "@", 1),       # typo
            orig["First_Name"][0] + ". " + orig["Last_Name"],  # initial format
            orig["Full_Name"] + " Jr.",
            orig["Full_Name"].replace("son", "son "),     # trailing space
        ]
        new_name = random.choice(name_variants[1:])  # never exact same
        first, *rest = new_name.split()
        last = rest[-1] if rest else orig["Last_Name"]

        dupe = orig.copy()
        dupe["Id"]           = str(900000 + len(dupes)).zfill(6)
        dupe["Full_Name"]    = new_name
        dupe["First_Name"]   = first
        dupe["Last_Name"]    = last
        dupe["Email"]        = f"{first.lower()}.{last.lower()}@{orig['Email_Domain']}"
        dupe["is_dirty"]     = True
        dupe["dirty_reason"] = f"duplicate_of:{orig['Id']}"
        dupes.append(dupe)
    return records + dupes


def inject_null_fields(records: list, n: int = 354) -> list:
    """Set mandatory fields to None on n randomly chosen records."""
    mandatory = ["Email", "Phone", "Account_Type", "Industry"]
    indices = random.sample(range(len(records)), n)
    for idx in indices:
        field = random.choice(mandatory)
        records[idx][field] = None
        records[idx]["is_dirty"] = True
        records[idx]["dirty_reason"] += f"|null:{field}"
    return records


def inject_bad_emails(records: list, n: int = 118) -> list:
    """Corrupt email addresses on n records."""
    bad_patterns = [
        lambda e: e.replace("@", ""),           # missing @
        lambda e: e.replace(".", "", 1),         # missing dot in domain
        lambda e: e + "..",                      # trailing dots
        lambda e: e.replace("@", "@@"),          # double @
        lambda e: re.sub(r"@\w+", "@", e),       # missing domain name
    ]
    indices = random.sample(range(len(records)), n)
    for idx in indices:
        if records[idx]["Email"] is None:
            continue
        fn = random.choice(bad_patterns)
        records[idx]["Email"] = fn(records[idx]["Email"])
        records[idx]["is_dirty"] = True
        records[idx]["dirty_reason"] += "|bad_email"
    return records


def inject_invalid_phones(records: list, n: int = 74) -> list:
    """Replace phone with malformed strings on n records."""
    bad_phones = [
        "123-456",          # too short
        "555.555",          # incomplete
        "CALL ME",          # text
        "(000) 000-0000",   # placeholder
        "n/a",
        "",
        "555",
    ]
    indices = random.sample(range(len(records)), n)
    for idx in indices:
        if records[idx]["Phone"] is None:
            continue
        records[idx]["Phone"] = random.choice(bad_phones)
        records[idx]["is_dirty"] = True
        records[idx]["dirty_reason"] += "|bad_phone"
    return records


def inject_invalid_states(records: list, n: int = 40) -> list:
    """Set invalid 2-letter state codes on n records."""
    indices = random.sample(range(len(records)), n)
    for idx in indices:
        records[idx]["State"] = random.choice(INVALID_STATES)
        records[idx]["is_dirty"] = True
        records[idx]["dirty_reason"] += "|invalid_state"
    return records


def inject_stale_records(records: list, n: int = 70) -> list:
    """Set Last_Activity_Date > 12 months ago on n records."""
    indices = random.sample(range(len(records)), n)
    for idx in indices:
        stale_date = fake.date_between(start_date="-5y", end_date="-13m")
        records[idx]["Last_Activity_Date"] = str(stale_date)
        records[idx]["is_dirty"] = True
        records[idx]["dirty_reason"] += "|stale"
    return records


def inject_missing_segments(records: list, n: int = 100) -> list:
    """Null out the Segment field on n records."""
    indices = random.sample(range(len(records)), n)
    for idx in indices:
        records[idx]["Segment"] = None
        records[idx]["is_dirty"] = True
        records[idx]["dirty_reason"] += "|null:Segment"
    return records


def inject_duplicate_domains(records: list, n_companies: int = 15) -> list:
    """Make n_companies share an email domain across multiple contacts."""
    shared_domains = [fake.domain_name() for _ in range(n_companies)]
    for i, domain in enumerate(shared_domains):
        # Pick 2–4 records to share this domain
        count = random.randint(2, 4)
        indices = random.sample(range(len(records)), count)
        for idx in indices:
            orig_email = records[idx]["Email"] or "contact@example.com"
            local = orig_email.split("@")[0]
            records[idx]["Email_Domain"] = domain
            if records[idx]["Email"]:
                records[idx]["Email"] = f"{local}@{domain}"
            records[idx]["is_dirty"] = True
            records[idx]["dirty_reason"] += f"|dup_domain:{domain}"
    return records


# ── Main ───────────────────────────────────────────────────────────────────────

def generate(n_records: int = 5200, output_path: str = "data/raw_contacts.csv") -> pd.DataFrame:
    print(f"[generate_data] Generating {n_records} clean base records...")
    records = [make_clean_record(i + 1) for i in range(n_records)]

    print("[generate_data] Injecting dirty data...")
    records = inject_duplicates(records,        n=127)
    records = inject_null_fields(records,       n=354)
    records = inject_bad_emails(records,        n=118)
    records = inject_invalid_phones(records,    n=74)
    records = inject_invalid_states(records,    n=40)
    records = inject_stale_records(records,     n=70)
    records = inject_missing_segments(records,  n=100)
    records = inject_duplicate_domains(records, n_companies=15)

    df = pd.DataFrame(records)
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)  # shuffle

    df.to_csv(output_path, index=False)

    # ── Summary ──
    total = len(df)
    dirty = df["is_dirty"].sum()
    print(f"\n{'─'*50}")
    print(f"  Total records written : {total:,}")
    print(f"  Clean records         : {total - dirty:,}")
    print(f"  Records with issues   : {dirty:,}  ({dirty/total*100:.1f}%)")
    print(f"  Output                : {output_path}")
    print(f"{'─'*50}\n")
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic CRM data")
    parser.add_argument("--records", type=int, default=5200, help="Number of base records")
    parser.add_argument("--out",     type=str, default="data/raw_contacts.csv", help="Output CSV path")
    args = parser.parse_args()

    import os
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    generate(n_records=args.records, output_path=args.out)