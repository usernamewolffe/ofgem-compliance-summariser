# tools/seed_iso_controls.py
"""
Seed a pragmatic subset of ISO/IEC controls into the `controls` table.

- ISO/IEC 27001:2022 (Annex A)  -> framework="ISO27001", version="2022"
- ISO/IEC 27019 (energy sector) -> framework="ISO27019", version="2017/23"

Notes:
- Control names are short and generic to avoid copyrighted verbatim text.
- Refs follow common conventions (e.g., "A.5.1", "A.8.16"). For 27019 we use
  "27019-x.y" style refs as practical anchors for mapping.
"""

from storage.db import DB

def main():
    db = DB("ofgem.db")

    ISO27001 = [
        # --- Organisation (A.5) ---
        {"ref": "A.5.1", "name": "Information security policy",
         "description": "Define, approve and communicate an IS policy.",
         "themes": "policy", "keywords": ["policy","information security policy","governance","approval","review"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.5.2", "name": "Roles and responsibilities",
         "description": "Assign IS roles and responsibilities.",
         "themes": "governance", "keywords": ["roles","responsibilities","accountability","RACI","ownership"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.5.9", "name": "Inventory of information and assets",
         "description": "Maintain inventories for information and supporting assets.",
         "themes": "assets", "keywords": ["asset","inventory","CMDB","classification","ownership"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.5.12", "name": "Data classification",
         "description": "Classify information by sensitivity and criticality.",
         "themes": "data", "keywords": ["classification","sensitivity","criticality","label","handling"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.5.14", "name": "Supplier security",
         "description": "Address IS in supplier lifecycle.",
         "themes": "supply chain", "keywords": ["supplier","third party","contract","due diligence","assurance","SBOM"],
         "framework": "ISO27001", "version": "2022"},

        # --- People (A.6) ---
        {"ref": "A.6.1", "name": "Screening and onboarding",
         "description": "Background checks and secure onboarding.",
         "themes": "people", "keywords": ["screening","onboarding","pre-employment","contractor"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.6.3", "name": "Awareness and training",
         "description": "Provide IS awareness and training.",
         "themes": "training", "keywords": ["training","awareness","phishing","exercise","simulation","culture"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.6.7", "name": "Disciplinary process",
         "description": "Formal actions for policy breaches.",
         "themes": "people", "keywords": ["disciplinary","sanctions","policy breach","misconduct"],
         "framework": "ISO27001", "version": "2022"},

        # --- Physical (A.7) ---
        {"ref": "A.7.1", "name": "Physical security perimeters",
         "description": "Define and protect secure areas.",
         "themes": "physical", "keywords": ["secure area","perimeter","fence","gate","CCTV","badge"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.7.3", "name": "Equipment security",
         "description": "Protect assets from threats and hazards.",
         "themes": "physical", "keywords": ["equipment","rack","environmental","UPS","HVAC","fire suppression"],
         "framework": "ISO27001", "version": "2022"},

        # --- Technological (A.8) ---
        {"ref": "A.8.2", "name": "Access management",
         "description": "Provisioning, review, and removal of access.",
         "themes": "access", "keywords": ["IAM","access","least privilege","MFA","joiner mover leaver","PAM"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.8.9", "name": "Configuration and hardening",
         "description": "Secure baseline configurations and changes.",
         "themes": "technology", "keywords": ["baseline","hardening","CIS","change control","build standard"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.8.10", "name": "Malware protection",
         "description": "Detect and prevent malware.",
         "themes": "technology", "keywords": ["malware","EDR","anti-virus","sandbox","quarantine"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.8.11", "name": "Logging and monitoring",
         "description": "Generate and review security logs.",
         "themes": "monitoring", "keywords": ["logging","SIEM","alert","audit log","retention","UEBA"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.8.12", "name": "Clock sync",
         "description": "Synchronise system clocks.",
         "themes": "technology", "keywords": ["ntp","time sync","timestamp","logs"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.8.13", "name": "Backup",
         "description": "Backup, test, and protect data.",
         "themes": "resilience", "keywords": ["backup","restore","immutability","3-2-1","retention","DR"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.8.16", "name": "Cryptography",
         "description": "Use of cryptographic controls.",
         "themes": "crypto", "keywords": ["encryption","key management","TLS","certificate","HSM"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.8.23", "name": "Web filtering and email security",
         "description": "Protect against phishing and web threats.",
         "themes": "technology", "keywords": ["phishing","email security","web proxy","DMARC","DKIM","SPF"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.8.28", "name": "Secure development",
         "description": "Security in the SDLC and changes.",
         "themes": "devsecops", "keywords": ["sdlc","code review","SAST","DAST","secrets","pipeline"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.8.34", "name": "Information deletion",
         "description": "Secure deletion of data/media.",
         "themes": "data", "keywords": ["data destruction","wiping","sanitisation","media disposal"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.8.35", "name": "Data masking",
         "description": "Mask and anonymise data where needed.",
         "themes": "data", "keywords": ["masking","anonymisation","tokenisation","privacy"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.8.36", "name": "Data leakage prevention",
         "description": "Prevent unauthorised disclosure.",
         "themes": "data", "keywords": ["DLP","exfiltration","egress","USB control","email DLP"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.8.34", "name": "Vulnerability management",
         "description": "Identify and treat vulnerabilities.",
         "themes": "vulnerability", "keywords": ["scanning","patching","CVSS","remediation","threat intel"],
         "framework": "ISO27001", "version": "2022"},

        # --- Response/continuity (cross-cutting but common in Annex A) ---
        {"ref": "A.5.IR", "name": "Incident management",
         "description": "Report, assess, respond and learn from incidents.",
         "themes": "incident", "keywords": ["incident","response","playbook","major incident","72 hours","post-incident"],
         "framework": "ISO27001", "version": "2022"},
        {"ref": "A.5.BC", "name": "Business continuity and DR",
         "description": "Plan and test for continuity and recovery.",
         "themes": "resilience", "keywords": ["BCP","DR","exercise","RTO","RPO","failover"],
         "framework": "ISO27001", "version": "2022"},
    ]

    ISO27019 = [
        {"ref": "27019-5.1", "name": "ICS security policy",
         "description": "Policy specific to industrial control systems.",
         "themes": "OT policy", "keywords": ["ICS","OT","policy","process control","automation"],
         "framework": "ISO27019", "version": "2017/23"},
        {"ref": "27019-7.1", "name": "OT asset inventory",
         "description": "Maintain inventory of control system assets and networks.",
         "themes": "OT assets", "keywords": ["PLC","RTU","IED","HMI","SCADA","DCS","asset inventory"],
         "framework": "ISO27019", "version": "2017/23"},
        {"ref": "27019-8.1", "name": "Network segmentation for OT",
         "description": "Separate and protect control networks.",
         "themes": "OT network", "keywords": ["segmentation","zones","conduits","DMZ","firewall","unidirectional gateway"],
         "framework": "ISO27019", "version": "2017/23"},
        {"ref": "27019-8.2", "name": "Remote access to OT",
         "description": "Control and monitor remote access into control systems.",
         "themes": "OT access", "keywords": ["remote access","supplier access","jump host","PAM","MFA"],
         "framework": "ISO27019", "version": "2017/23"},
        {"ref": "27019-8.3", "name": "Patch and change in OT",
         "description": "Risk-based patching and change coordination for OT.",
         "themes": "OT change", "keywords": ["patching","change window","vendor approval","baseline"],
         "framework": "ISO27019", "version": "2017/23"},
        {"ref": "27019-8.4", "name": "OT monitoring and logging",
         "description": "Log sources and anomaly detection for control systems.",
         "themes": "OT monitoring", "keywords": ["syslog","span port","ICS IDS","DPI","anomaly","SIEM"],
         "framework": "ISO27019", "version": "2017/23"},
        {"ref": "27019-8.5", "name": "Backup and recovery for OT",
         "description": "Back up configurations and data; test restore.",
         "themes": "OT resilience", "keywords": ["backup","config","gold image","restore","offline copy"],
         "framework": "ISO27019", "version": "2017/23"},
        {"ref": "27019-9.1", "name": "Supplier and integrator controls",
         "description": "Assure security across vendors, integrators, and service providers.",
         "themes": "OT supply chain", "keywords": ["integrator","vendor","maintenance","contractor","procurement"],
         "framework": "ISO27019", "version": "2017/23"},
        {"ref": "27019-9.2", "name": "Engineering workstation security",
         "description": "Harden and control engineering/maintenance workstations.",
         "themes": "OT endpoints", "keywords": ["EWS","PAM","hardening","whitelisting","removable media"],
         "framework": "ISO27019", "version": "2017/23"},
        {"ref": "27019-9.3", "name": "Removable media handling (OT)",
         "description": "Control removable media across OT environments.",
         "themes": "OT media", "keywords": ["USB","removable media","transfer station","scanning","allow list"],
         "framework": "ISO27019", "version": "2017/23"},
    ]

    for c in ISO27001 + ISO27019:
        cid = db.upsert_control(
            ref=c["ref"],
            name=c["name"],
            description=c.get("description",""),
            themes=c.get("themes",""),
            keywords=c.get("keywords",[]),
            framework=c.get("framework"),
            version=c.get("version"),
        )
        print(f"Upserted {c['framework']} {c['ref']} -> {cid}")

    print("Done.")

if __name__ == "__main__":
    main()
