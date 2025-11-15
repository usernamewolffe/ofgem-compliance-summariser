from storage.db import DB
db = DB("ofgem.db")
SEED = [
  {"ref":"CAF-A1","name":"Governance & Leadership","framework":"CAF","version":"v3",
   "description":"Clear accountability for cyber resilience.",
   "themes":"governance","keywords":["governance","leadership","board","accountability","management responsibility"]},
  {"ref":"CAF-A2","name":"Risk Management","framework":"CAF","version":"v3",
   "description":"Risk management processes are established and effective.",
   "themes":"risk","keywords":["risk","risk assessment","mitigation","threat","vulnerability","register"]},
  {"ref":"CAF-A3","name":"Asset Management","framework":"CAF","version":"v3",
   "description":"Critical assets are identified and managed.",
   "themes":"assets","keywords":["asset","asset register","inventory","CMDB","configuration management"]},
  {"ref":"CAF-B2","name":"Identity & Access Control","framework":"CAF","version":"v3",
   "description":"Access to networks/systems is managed and restricted.",
   "themes":"access","keywords":["access","authentication","authorisation","least privilege","MFA","privileged","PAM"]},
  {"ref":"CAF-C1","name":"Security Monitoring","framework":"CAF","version":"v3",
   "description":"Events and logs monitored to detect incidents.",
   "themes":"monitoring","keywords":["monitoring","logging","SIEM","detect","anomaly","alert"]},
  {"ref":"CAF-D1","name":"Response & Recovery Planning","framework":"CAF","version":"v3",
   "description":"Respond and recover within required timeframes.",
   "themes":"incident","keywords":["incident","incident response","major incident","notification","72 hours","recovery","playbook","communication"]},
]
for c in SEED:
    print("Upserted", c["ref"], "->", db.upsert_control(**c))


