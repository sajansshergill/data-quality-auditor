# Salesforce CRM Data Quality Auditor

An automated Python pipeline that profiles, audits and cleans CRM contact data –– surfacing duplicates, null fields, format violations, and stale records through a Streamlit dashboard with downloadable audit reports.

## Motivation
CRM databases degrade silently. Duplicate contacts get created when reps add records manually. Mnadaroty fields like Account_Type get skipped during bulk imports. Phone numbers come on five deifferent formats. After 12 months of no activity, accounts sit stale and skew segmentation.

This project simulates the full data stewardship lifecycle for a Salesforce-style CRM export: ingest -> profile -> detect issues -> resolve -> report. Every step maps directly to real data admin responsibilities; running quality checks, merging duplicates, standardizing formats, and handling off a clean audit log to the team.

## Demo
<img width="596" height="342" alt="image" src="https://github.com/user-attachments/assets/865ab637-604d-4547-8599-30d77caed850" />

## Features
- Synthetic CRM data generation - Faker-powered generator produces 5,200 realistic contact records wiht seeded dirty data (duplicates, nulls, bad formats, stale accounts)
- 8 automated quality checks - null detection, fuzzy deduplication, email/phone validation, US state validation, duplicate domain detection, stale-record flagging, and missing segmentation tags
- Fuzzy deduplication - rapidfuzz token-sort ratio matches name+email pairs above a configurable similarity threshold; keeps the higher-confidence record
- Resolution engine - applies fix rules: merges duplicates, standardizes phone/email formats, infers missing segments from account type, flags unresolvable rows for manual reveiew.
- Streamlit dashboard - before/after quality score cards, issues severity breakdown chart, per-field null heatmap, and full issues log
- Downloadable audit log - CSV export with record_id, issue_type, severity, field, original_value, reolved_value, resolution_action columns - ready for team handoff
- Optional live Salesforce mode - swap synthetic CSV for a real Salesforce sandbox org via simple-salesforc

## Project Structure
<img width="580" height="433" alt="image" src="https://github.com/user-attachments/assets/13f43560-c197-47dc-92a8-7089a866ac3a" />

## Quickstart
### 1. Clone and install
bashgit clone https://github.com/sajanshergill/salesforce-data-quality-auditor.git
cd salesforce-data-quality-auditor
pip install -r requirements.txt

### 2. Generate synthetic CRM data
bashpython src/generate_data.py --records 5200 --out data/raw_contacts.csv
This creates a dirty CSV with seeded issues: ~2.4% duplicates, ~6.8% null mandatory fields, ~2.3% malformed emails, and ~1.9% stale accounts.

### 3. Run the audit
bashpython src/audit.py --input data/raw_contacts.csv --out data/audit_log.csv
Prints a summary to stdout and writes the full issue log to data/audit_log.csv.

### 4. Run the cleanup
bashpython src/clean.py --input data/raw_contacts.csv --audit data/audit_log.csv --out data/cleaned_contacts.csv

### 5. Launch the dashboard
bashstreamlit run src/app.py
Open http://localhost:8501 in your browser.

## Quality Checks

<img width="663" height="394" alt="image" src="https://github.com/user-attachments/assets/bd50a79e-647a-4afe-adea-e2269e665e65" />

## Configuration
All thresholds live in src/config.py:
DUPLICATE_SIMILARITY_THRESHOLD = 90 # rapidfuzz score (0-100)
STALE_RECORD_MONTHS = 12 # months since last activity
MANDATORY_FIELDS = ["Email", "Phone", "Account_Type", "Industry"]
VALID_SEGEMENTS = ["Enterprise", "SMB", "Startup", "Non-profit", "Government"]

## Docker
docker build -t crm--auditor
docker run -p 8501:8501 crm-auditor

## Tech Stack 
<img width="454" height="359" alt="image" src="https://github.com/user-attachments/assets/bae42abf-7047-477c-aa6c-04c5939b1ae7" />

## Sample Audio Log Output
record_id | issue_type  | severity | field        | original_value           | resolved_value           | resolution_action
1042      | duplicate   | critical | Name/Email   | John Smith / j@acme.com  | —                        | merged into record 3871
2201      | bad_email   | medium   | Email        | sarah.jones@gmailcom     | sarah.jones@gmail.com    | auto-corrected
4819      | null_field  | high     | Account_Type | NULL                     | SMB                      | inferred from Industry
3301      | stale       | low      | Last_Activity| 2022-11-03               | —                        | flagged for manual review

## What I learned
- Real CRM deduplication is messier than exact-match - fuzzy matching on name+email pairs catches the cases that slip through (typos, nicknames, domain variants)
- Seevrity tiering matters for team handoffs: critical issues (duplicates) need immediate action; low-severity flags (stale records) need human judgement, not automation
- A downloadable audit log CSV is more useful than a pretty dashboard alone –– it's what a data admin actually hands to a manager
- simple-salesforce makes it straightforward to swap a synthetic dataset for a real org with minimal code changes.



