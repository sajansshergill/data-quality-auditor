"""
clean.py
Applies fix rules to a raw CRM CSV based on the audit log from audit.py.

Resolution logic per issue type:
  null_field        → infer from related fields if possible; else flag for manual review
  duplicate         → keep higher-confidence record; mark loser as merged
  bad_email         → attempt auto-correction (common patterns); else null + flag
  bad_phone         → strip to digits, reformat to (###) ###-####; else null + flag
  invalid_state     → null + flag for manual review
  duplicate_domain  → flag only (no auto-resolution, needs human judgment)
  stale_record      → flag only (archive recommendation)
  missing_segment   → infer from Account_Type if available

Usage:
    python src/clean.py --input data/raw_contacts.csv \
                        --audit data/audit_log.csv \
                        --out data/cleaned_contacts.csv
"""

import argparse
import os
import re

import pandas as pd

from audit import quality_score, run_all_checks, MANDATORY_FIELDS, VALID_SEGMENTS

# ── Phone normalizer ───────────────────────────────────────────────────────────

def normalize_phone(raw: str) -> str | None:
    """Strip non-digits and reformat to (###) ###-#### if 10 digits result."""
    digits = re.sub(r"\D", "", str(raw))
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return None


# ── Email auto-corrector ───────────────────────────────────────────────────────

COMMON_DOMAIN_FIXES = {
    "gmailcom":   "gmail.com",
    "gmailco":    "gmail.com",
    "gmai.com":   "gmail.com",
    "yahoocom":   "yahoo.com",
    "yahoocon":   "yahoo.com",
    "hotmailcom": "hotmail.com",
    "outlookcom": "outlook.com",
}

EMAIL_RE = re.compile(r"^[\w\.\+\-]+@[\w\-]+\.[a-z]{2,}$")

def fix_email(raw: str) -> str | None:
    """Attempt common email corrections; return None if unresolvable."""
    val = str(raw).strip().lower()

    # Fix double @@
    val = re.sub(r"@{2,}", "@", val)
    # Fix trailing dots
    val = val.rstrip(".")
    # Fix missing dot before known TLD (e.g. gmailcom → gmail.com)
    if "@" in val:
        local, domain = val.rsplit("@", 1)
        domain = COMMON_DOMAIN_FIXES.get(domain, domain)
        # If domain has no dot, try inserting one before last 3 chars
        if "." not in domain and len(domain) > 3:
            domain = domain[:-3] + "." + domain[-3:]
        val = f"{local}@{domain}"
    else:
        return None

    return val if EMAIL_RE.match(val) else None


# ── Duplicate resolution ───────────────────────────────────────────────────────

def resolve_duplicates(df: pd.DataFrame, audit_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    For each duplicate pair in the audit log:
      - Keep the record with fewer null fields (higher completeness)
      - Drop the loser
      - Update audit log with resolution_action
    """
    # Normalise IDs to zero-padded 6-char strings to match df["Id"]
    def _fmt(val):
        try:
            return str(int(val)).zfill(6)
        except (ValueError, TypeError):
            return str(val)

    dup_issues = audit_df[audit_df["issue_type"] == "duplicate"].copy()
    ids_to_drop = set()

    for idx, row in dup_issues.iterrows():
        record_id = _fmt(row["record_id"])
        note      = row["note"]

        # Parse the matched record id from note: "Matches record XXXXXX ..."
        match = re.search(r"Matches record (\w+)", note)
        if not match:
            continue
        other_id = _fmt(match.group(1))

        if record_id in ids_to_drop or other_id in ids_to_drop:
            continue  # already resolved as part of another pair

        rec_a = df[df["Id"] == record_id]
        rec_b = df[df["Id"] == other_id]

        if rec_a.empty or rec_b.empty:
            continue

        # Count nulls per record — keep the one with fewer nulls
        nulls_a = rec_a.iloc[0][MANDATORY_FIELDS].isnull().sum()
        nulls_b = rec_b.iloc[0][MANDATORY_FIELDS].isnull().sum()

        if nulls_a <= nulls_b:
            loser = other_id
            winner = record_id
        else:
            loser = record_id
            winner = other_id

        ids_to_drop.add(loser)

        audit_df.loc[idx, "resolved_value"]    = f"Merged into record {winner}"
        audit_df.loc[idx, "resolution_action"] = "auto_merged"

    df_clean = df[~df["Id"].isin(ids_to_drop)].copy()
    return df_clean, audit_df


# ── Null field resolution ──────────────────────────────────────────────────────

def resolve_nulls(df: pd.DataFrame, audit_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    null_issues = audit_df[audit_df["issue_type"] == "null_field"].copy()

    for idx, row in null_issues.iterrows():
        record_id = row["record_id"]
        field     = row["field"]
        df_idx    = df[df["Id"] == record_id].index

        if df_idx.empty:
            continue
        i = df_idx[0]

        # Infer Segment from Account_Type
        if field == "Segment":
            acct_type = df.at[i, "Account_Type"]
            if pd.notna(acct_type) and acct_type in VALID_SEGMENTS:
                df.at[i, "Segment"] = acct_type
                audit_df.loc[idx, "resolved_value"]    = acct_type
                audit_df.loc[idx, "resolution_action"] = "inferred_from_account_type"
                continue

        # Infer Account_Type from Segment (reverse)
        if field == "Account_Type":
            segment = df.at[i, "Segment"]
            if pd.notna(segment) and segment in VALID_SEGMENTS:
                df.at[i, "Account_Type"] = segment
                audit_df.loc[idx, "resolved_value"]    = segment
                audit_df.loc[idx, "resolution_action"] = "inferred_from_segment"
                continue

        # No auto-fix available
        audit_df.loc[idx, "resolution_action"] = "flagged_manual_review"

    return df, audit_df


# ── Email fix ─────────────────────────────────────────────────────────────────

def resolve_emails(df: pd.DataFrame, audit_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    email_issues = audit_df[audit_df["issue_type"] == "bad_email"].copy()

    for idx, row in email_issues.iterrows():
        record_id = row["record_id"]
        df_idx    = df[df["Id"] == record_id].index
        if df_idx.empty:
            continue
        i = df_idx[0]

        raw = df.at[i, "Email"]
        fixed = fix_email(str(raw))

        if fixed:
            df.at[i, "Email"] = fixed
            audit_df.loc[idx, "resolved_value"]    = fixed
            audit_df.loc[idx, "resolution_action"] = "auto_corrected"
        else:
            df.at[i, "Email"] = None
            audit_df.loc[idx, "resolved_value"]    = "NULL"
            audit_df.loc[idx, "resolution_action"] = "nulled_flagged_manual_review"

    return df, audit_df


# ── Phone fix ─────────────────────────────────────────────────────────────────

def resolve_phones(df: pd.DataFrame, audit_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    phone_issues = audit_df[audit_df["issue_type"] == "bad_phone"].copy()

    for idx, row in phone_issues.iterrows():
        record_id = row["record_id"]
        df_idx    = df[df["Id"] == record_id].index
        if df_idx.empty:
            continue
        i = df_idx[0]

        raw = df.at[i, "Phone"]
        fixed = normalize_phone(str(raw))

        if fixed:
            df.at[i, "Phone"] = fixed
            audit_df.loc[idx, "resolved_value"]    = fixed
            audit_df.loc[idx, "resolution_action"] = "auto_reformatted"
        else:
            df.at[i, "Phone"] = None
            audit_df.loc[idx, "resolved_value"]    = "NULL"
            audit_df.loc[idx, "resolution_action"] = "nulled_flagged_manual_review"

    return df, audit_df


# ── State fix ────────────────────────────────────────────────────────────────

def resolve_states(df: pd.DataFrame, audit_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    state_issues = audit_df[audit_df["issue_type"] == "invalid_state"].copy()

    for idx, row in state_issues.iterrows():
        record_id = row["record_id"]
        df_idx    = df[df["Id"] == record_id].index
        if df_idx.empty:
            continue
        i = df_idx[0]

        df.at[i, "State"] = None
        audit_df.loc[idx, "resolved_value"]    = "NULL"
        audit_df.loc[idx, "resolution_action"] = "nulled_flagged_manual_review"

    return df, audit_df


# ── Segment fill ──────────────────────────────────────────────────────────────

def resolve_segments(df: pd.DataFrame, audit_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    seg_issues = audit_df[audit_df["issue_type"] == "missing_segment"].copy()

    for idx, row in seg_issues.iterrows():
        record_id = row["record_id"]
        df_idx    = df[df["Id"] == record_id].index
        if df_idx.empty:
            continue
        i = df_idx[0]

        acct_type = df.at[i, "Account_Type"]
        if pd.notna(acct_type) and acct_type in VALID_SEGMENTS:
            df.at[i, "Segment"] = acct_type
            audit_df.loc[idx, "resolved_value"]    = acct_type
            audit_df.loc[idx, "resolution_action"] = "inferred_from_account_type"
        else:
            audit_df.loc[idx, "resolution_action"] = "flagged_manual_review"

    return df, audit_df


# ── Flag-only issues (no auto-fix) ────────────────────────────────────────────

def flag_only(audit_df: pd.DataFrame, issue_types: list) -> pd.DataFrame:
    audit_df["resolution_action"] = audit_df["resolution_action"].astype(str).replace("nan", "")
    mask = audit_df["issue_type"].isin(issue_types)
    audit_df.loc[mask & (audit_df["resolution_action"] == ""), "resolution_action"] = "flagged_manual_review"
    return audit_df


# ── Main cleaner ──────────────────────────────────────────────────────────────

def clean(df: pd.DataFrame, audit_df: pd.DataFrame, verbose: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    original_count = len(df)

    # Ensure string columns don't get read as float64 (NaN rows cause dtype issues)
    for col in ["resolved_value", "resolution_action", "note"]:
        if col in audit_df.columns:
            audit_df[col] = audit_df[col].fillna("").astype(str)

    if verbose:
        print("[clean] Resolving duplicates...")
    df, audit_df = resolve_duplicates(df, audit_df)
    merged = original_count - len(df)

    if verbose:
        print(f"        → {merged} duplicate records merged/removed")
        print("[clean] Resolving null fields...")
    df, audit_df = resolve_nulls(df, audit_df)

    if verbose:
        print("[clean] Fixing malformed emails...")
    df, audit_df = resolve_emails(df, audit_df)

    if verbose:
        print("[clean] Normalizing phone numbers...")
    df, audit_df = resolve_phones(df, audit_df)

    if verbose:
        print("[clean] Nulling invalid states...")
    df, audit_df = resolve_states(df, audit_df)

    if verbose:
        print("[clean] Filling missing segments...")
    df, audit_df = resolve_segments(df, audit_df)

    audit_df = flag_only(audit_df, ["stale_record", "duplicate_domain"])

    if verbose:
        print(f"\n{'─'*55}")
        print(f"  Records before : {original_count:,}")
        print(f"  Records after  : {len(df):,}")
        print(f"  Merged/removed : {merged:,}")
        resolved = (audit_df["resolution_action"] != "flagged_manual_review").sum()
        manual   = (audit_df["resolution_action"] == "flagged_manual_review").sum()
        print(f"  Issues auto-resolved  : {resolved:,}")
        print(f"  Issues need review    : {manual:,}")

        # Re-run audit on cleaned data for after score
        print("\n[clean] Scoring cleaned dataset...")
        audit_after = run_all_checks(df, verbose=False)
        score_after = quality_score(df, audit_after)
        print(f"  Quality score (after) : {score_after}%")
        print(f"{'─'*55}\n")

    return df, audit_df


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean CRM data based on audit log")
    parser.add_argument("--input", type=str, default="data/raw_contacts.csv")
    parser.add_argument("--audit", type=str, default="data/audit_log.csv")
    parser.add_argument("--out",   type=str, default="data/cleaned_contacts.csv")
    parser.add_argument("--audit-out", type=str, default="data/audit_log_resolved.csv")
    args = parser.parse_args()

    print(f"\n[clean] Loading {args.input}...")
    df = pd.read_csv(args.input, dtype=str)

    print(f"[clean] Loading audit log {args.audit}...")
    audit_df = pd.read_csv(args.audit, dtype={"resolved_value": str, "resolution_action": str, "note": str})

    print()
    df_clean, audit_resolved = clean(df, audit_df, verbose=True)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df_clean.to_csv(args.out, index=False)
    audit_resolved.to_csv(args.audit_out, index=False)

    print(f"[clean] Cleaned data  → {args.out}")
    print(f"[clean] Resolved log  → {args.audit_out}\n")