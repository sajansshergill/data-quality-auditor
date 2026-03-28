"""
audit.py
Runs 8 automated data quality checks on a CRM contact CSV and writes an audit log.

Usage:
    python src/audit.py --input data/raw_contacts.csv --out data/audit_log.csv
"""

import argparse
import os
import re
from datetime import datetime, timedelta

import pandas as pd
from rapidfuzz import fuzz

# ── Config ─────────────────────────────────────────────────────────────────────

MANDATORY_FIELDS = ["Email", "Phone", "Account_Type", "Industry"]
VALID_SEGMENTS   = ["Enterprise", "SMB", "Startup", "Non-profit", "Government"]
US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY",
}
EMAIL_RE                  = re.compile(r"^[\w\.\+\-]+@[\w\-]+\.[a-z]{2,}$")
PHONE_RE                  = re.compile(r"^\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]\d{4}$")
DUPLICATE_THRESHOLD       = 90    # rapidfuzz score 0–100
STALE_MONTHS              = 12


# ── Issue builder ──────────────────────────────────────────────────────────────

def _issue(record_id, issue_type, severity, field, original_value, note=""):
    return {
        "record_id":      record_id,
        "issue_type":     issue_type,
        "severity":       severity,
        "field":          field,
        "original_value": str(original_value)[:120] if original_value is not None else "NULL",
        "resolved_value": "",          # filled by clean.py
        "resolution_action": "",       # filled by clean.py
        "note":           note,
    }


# ── Check 1 — Null mandatory fields ───────────────────────────────────────────

def check_nulls(df: pd.DataFrame) -> list:
    issues = []
    for field in MANDATORY_FIELDS:
        null_rows = df[df[field].isnull()]
        for _, row in null_rows.iterrows():
            issues.append(_issue(row["Id"], "null_field", "high", field, None,
                                 f"Mandatory field '{field}' is missing"))
    return issues


# ── Check 2 — Fuzzy duplicate detection ───────────────────────────────────────

def check_duplicates(df: pd.DataFrame) -> list:
    """
    Compare every pair of records on Full_Name + Email using
    rapidfuzz token_sort_ratio. O(n²) — fine for datasets up to ~10K.
    For larger datasets, block on Email_Domain first.
    """
    issues = []
    ids    = df["Id"].tolist()
    names  = df["Full_Name"].fillna("").tolist()
    emails = df["Email"].fillna("").tolist()

    seen_pairs = set()

    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            pair = (min(ids[i], ids[j]), max(ids[i], ids[j]))
            if pair in seen_pairs:
                continue

            name_score  = fuzz.token_sort_ratio(names[i],  names[j])
            email_score = fuzz.token_sort_ratio(emails[i], emails[j])
            combined    = (name_score * 0.6) + (email_score * 0.4)

            if combined >= DUPLICATE_THRESHOLD:
                seen_pairs.add(pair)
                issues.append(_issue(
                    ids[i], "duplicate", "critical",
                    "Full_Name/Email",
                    f"{names[i]} / {emails[i]}",
                    f"Matches record {ids[j]} (similarity: {combined:.0f}%)"
                ))
    return issues


# ── Check 3 — Malformed email ─────────────────────────────────────────────────

def check_email_format(df: pd.DataFrame) -> list:
    issues = []
    has_email = df[df["Email"].notna()]
    bad = has_email[~has_email["Email"].str.match(EMAIL_RE, na=False)]
    for _, row in bad.iterrows():
        issues.append(_issue(row["Id"], "bad_email", "medium",
                             "Email", row["Email"],
                             "Email address does not match expected format"))
    return issues


# ── Check 4 — Invalid phone ───────────────────────────────────────────────────

def check_phone_format(df: pd.DataFrame) -> list:
    issues = []
    has_phone = df[df["Phone"].notna()]
    bad = has_phone[~has_phone["Phone"].astype(str).str.strip().str.match(PHONE_RE, na=False)]
    for _, row in bad.iterrows():
        val = str(row["Phone"]).strip()
        if val == "":
            continue
        issues.append(_issue(row["Id"], "bad_phone", "medium",
                             "Phone", row["Phone"],
                             "Phone does not match a valid US format"))
    return issues


# ── Check 5 — Invalid US state ────────────────────────────────────────────────

def check_state(df: pd.DataFrame) -> list:
    issues = []
    if "State" not in df.columns:
        return issues
    bad = df[df["State"].notna() & ~df["State"].isin(US_STATES)]
    for _, row in bad.iterrows():
        issues.append(_issue(row["Id"], "invalid_state", "medium",
                             "State", row["State"],
                             f"'{row['State']}' is not a valid US state code"))
    return issues


# ── Check 6 — Duplicate email domain ─────────────────────────────────────────

def check_duplicate_domains(df: pd.DataFrame) -> list:
    """Flag accounts sharing an email domain (potential duplicates or data entry errors)."""
    issues = []
    if "Email_Domain" not in df.columns:
        return issues
    domain_counts = df["Email_Domain"].value_counts()
    shared = domain_counts[domain_counts > 3].index  # >3 contacts = suspicious
    flagged = df[df["Email_Domain"].isin(shared)]
    for _, row in flagged.iterrows():
        count = domain_counts[row["Email_Domain"]]
        issues.append(_issue(row["Id"], "duplicate_domain", "low",
                             "Email_Domain", row["Email_Domain"],
                             f"{count} contacts share this domain — review for account merges"))
    return issues


# ── Check 7 — Stale records ───────────────────────────────────────────────────

def check_stale_records(df: pd.DataFrame) -> list:
    issues = []
    if "Last_Activity_Date" not in df.columns:
        return issues
    cutoff = datetime.today() - timedelta(days=STALE_MONTHS * 30)
    for _, row in df.iterrows():
        val = row["Last_Activity_Date"]
        if pd.isna(val):
            continue
        try:
            activity_date = datetime.strptime(str(val), "%Y-%m-%d")
            if activity_date < cutoff:
                issues.append(_issue(row["Id"], "stale_record", "low",
                                     "Last_Activity_Date", val,
                                     f"No activity since {val} — exceeds {STALE_MONTHS}-month threshold"))
        except ValueError:
            pass
    return issues


# ── Check 8 — Missing segmentation ───────────────────────────────────────────

def check_missing_segments(df: pd.DataFrame) -> list:
    issues = []
    if "Segment" not in df.columns:
        return issues
    bad = df[df["Segment"].isnull() | ~df["Segment"].isin(VALID_SEGMENTS)]
    for _, row in bad.iterrows():
        issues.append(_issue(row["Id"], "missing_segment", "medium",
                             "Segment", row.get("Segment"),
                             f"Segment is missing or not in allowed values: {VALID_SEGMENTS}"))
    return issues


# ── Runner ─────────────────────────────────────────────────────────────────────

CHECK_REGISTRY = [
    ("Null mandatory fields",   check_nulls),
    ("Fuzzy duplicates",        check_duplicates),
    ("Malformed emails",        check_email_format),
    ("Invalid phones",          check_phone_format),
    ("Invalid US states",       check_state),
    ("Duplicate email domains", check_duplicate_domains),
    ("Stale records",           check_stale_records),
    ("Missing segmentation",    check_missing_segments),
]


def run_all_checks(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    all_issues = []

    for label, fn in CHECK_REGISTRY:
        if verbose:
            print(f"  Running: {label}...", end=" ", flush=True)
        issues = fn(df)
        all_issues.extend(issues)
        if verbose:
            print(f"{len(issues)} issue(s) found")

    audit_df = pd.DataFrame(all_issues)

    if verbose and not audit_df.empty:
        print(f"\n{'─'*55}")
        print(f"  {'CHECK':<30} {'COUNT':>6}  {'SEVERITY'}")
        print(f"{'─'*55}")
        summary = audit_df.groupby(["issue_type", "severity"]).size().reset_index(name="count")
        for _, row in summary.iterrows():
            print(f"  {row['issue_type']:<30} {row['count']:>6}  {row['severity']}")
        print(f"{'─'*55}")
        print(f"  {'TOTAL':<30} {len(audit_df):>6}")
        print(f"{'─'*55}\n")

    return audit_df


def quality_score(df: pd.DataFrame, audit_df: pd.DataFrame) -> float:
    """
    Score = 1 - (weighted issue count / total fields checked).
    Critical = weight 3, high = 2, medium = 1, low = 0.5
    """
    weights = {"critical": 3, "high": 2, "medium": 1, "low": 0.5}
    total_fields = len(df) * len(MANDATORY_FIELDS + ["Email", "Phone", "State", "Segment"])
    weighted_issues = audit_df["severity"].map(weights).sum() if not audit_df.empty else 0
    score = max(0.0, 1.0 - (weighted_issues / total_fields))
    return round(score * 100, 1)


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run CRM data quality audit")
    parser.add_argument("--input", type=str, default="data/raw_contacts.csv")
    parser.add_argument("--out",   type=str, default="data/audit_log.csv")
    args = parser.parse_args()

    print(f"\n[audit] Loading {args.input}...")
    df = pd.read_csv(args.input, dtype=str)
    print(f"[audit] {len(df):,} records loaded.\n")

    print("[audit] Running quality checks...\n")
    audit_df = run_all_checks(df, verbose=True)

    score_before = quality_score(df, audit_df)
    print(f"  Quality score (before cleanup): {score_before}%\n")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    audit_df.to_csv(args.out, index=False)
    print(f"[audit] Audit log saved → {args.out}")
    print(f"[audit] Done. {len(audit_df):,} total issues logged.\n")