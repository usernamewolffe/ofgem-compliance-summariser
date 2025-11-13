# tools/seed_orgs_and_sites.py
from storage.db import DB
from datetime import datetime, timedelta, timezone

def main():
    db = DB("ofgem.db")

    # --- organisation & site ---
    org_name = "Example Energy Ltd"
    org_id = db.upsert_org(org_name)
    print(f"✅ Organisation: {org_name} (id={org_id})")

    site_name = "Leeds CHP Site"
    site_id = db.upsert_site(org_id, site_name)
    print(f"✅ Site: {site_name} (id={site_id})")

    # --- org-wide controls ---
    cid1 = db.upsert_org_control(
        org_id=org_id,
        title="Information Security Governance",
        code="ORG-SEC-01",
        description="Define and maintain an information security policy framework.",
        owner_email="sec.lead@exampleenergy.co.uk",
        tags=["policy", "governance"],
        status="Active",
        risk="Low",
        review_frequency_days=180,
        next_review_at=(datetime.now(timezone.utc) + timedelta(days=180)).isoformat(),
    )

    cid2 = db.upsert_org_control(
        org_id=org_id,
        title="Incident Management Procedure",
        code="ORG-INC-01",
        description="Maintain a tested process for detecting, reporting, and responding to security incidents.",
        owner_email="ops.manager@exampleenergy.co.uk",
        tags=["incident", "response"],
        status="Active",
        risk="Medium",
        review_frequency_days=90,
        next_review_at=(datetime.now(timezone.utc) + timedelta(days=90)).isoformat(),
    )

    # --- site-level control ---
    cid3 = db.upsert_org_control(
        org_id=org_id,
        site_id=site_id,
        title="On-Site Access Control",
        code="SITE-ACCESS-01",
        description="Physical access to control rooms must be restricted to authorised personnel only.",
        owner_email="site.manager@exampleenergy.co.uk",
        tags=["physical", "access"],
        status="Active",
        risk="Medium",
        review_frequency_days=365,
        next_review_at=(datetime.now(timezone.utc) + timedelta(days=365)).isoformat(),
    )

    print("\n✅ Created controls:")
    for c in db.list_all_controls_for_org(org_id):
        site_label = c["site_name"] or "Corporate"
        print(f"  - [{site_label}] {c['code']} — {c['title']}")

    print("\nDone. You can now view:")
    print(f"  • /orgs/{org_id}/controls  → overview page")
    print(f"  • /orgs/{org_id}/sites     → (if implemented later)")
    print()

if __name__ == "__main__":
    main()
