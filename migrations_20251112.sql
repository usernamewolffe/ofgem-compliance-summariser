-- ===============================
-- Core org/site structure
-- ===============================
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS orgs (
  id                      INTEGER PRIMARY KEY AUTOINCREMENT,
  name                    TEXT NOT NULL,
  head_office_address     TEXT,
  phone                   TEXT,
  email                   TEXT,
  website                 TEXT,
  created_at              TEXT DEFAULT (datetime('now')),
  updated_at              TEXT
);

CREATE TABLE IF NOT EXISTS sites (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  org_id      INTEGER NOT NULL,
  name        TEXT NOT NULL,
  address     TEXT,
  phone       TEXT,
  email       TEXT,
  created_at  TEXT DEFAULT (datetime('now')),
  updated_at  TEXT,
  FOREIGN KEY (org_id) REFERENCES orgs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_sites_org_id ON sites(org_id);
CREATE INDEX IF NOT EXISTS idx_sites_name    ON sites(name);

-- ===============================
-- Key personnel (org & site)
-- ===============================
CREATE TABLE IF NOT EXISTS org_members (
  id                        INTEGER PRIMARY KEY AUTOINCREMENT,
  org_id                    INTEGER NOT NULL,
  name                      TEXT NOT NULL,
  role                      TEXT,
  email                     TEXT,
  is_key_personnel          INTEGER DEFAULT 0,  -- 0/1
  is_ultimate_risk_owner    INTEGER DEFAULT 0,  -- 0/1
  created_at                TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (org_id) REFERENCES orgs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_org_members_org_id ON org_members(org_id);
CREATE INDEX IF NOT EXISTS idx_org_members_uro    ON org_members(org_id, is_ultimate_risk_owner DESC);

CREATE TABLE IF NOT EXISTS site_members (
  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id            INTEGER NOT NULL,
  name               TEXT NOT NULL,
  role               TEXT,
  email              TEXT,
  is_key_personnel   INTEGER DEFAULT 0,  -- 0/1
  created_at         TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_site_members_site_id ON site_members(site_id);

-- ===============================
-- Org-wide risks & controls
-- ===============================
CREATE TABLE IF NOT EXISTS org_risks (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  org_id        INTEGER NOT NULL,
  code          TEXT,                    -- e.g., R-001
  title         TEXT NOT NULL,
  description   TEXT,
  status        TEXT,                    -- Open / In progress / Mitigated / Closed
  severity      TEXT,                    -- Low / Medium / High / Severe
  category      TEXT,                    -- e.g., Cyber, Safety, Regulatory
  owner_name    TEXT,
  owner_email   TEXT,
  created_at    TEXT DEFAULT (datetime('now')),
  updated_at    TEXT,
  FOREIGN KEY (org_id) REFERENCES orgs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_org_risks_org_id     ON org_risks(org_id);
CREATE INDEX IF NOT EXISTS idx_org_risks_status     ON org_risks(status);
CREATE INDEX IF NOT EXISTS idx_org_risks_severity   ON org_risks(severity);
CREATE INDEX IF NOT EXISTS idx_org_risks_category   ON org_risks(category);

CREATE TABLE IF NOT EXISTS org_controls (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  org_id        INTEGER NOT NULL,
  code          TEXT,                    -- e.g., C-001
  title         TEXT NOT NULL,
  description   TEXT,
  owner_name    TEXT,
  owner_email   TEXT,
  status        TEXT,                    -- Optional: Active / Deprecated
  created_at    TEXT DEFAULT (datetime('now')),
  updated_at    TEXT,
  FOREIGN KEY (org_id) REFERENCES orgs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_org_controls_org_id ON org_controls(org_id);

-- Link table: org controls ↔ org risks (many-to-many)
CREATE TABLE IF NOT EXISTS org_controls_risks (
  org_control_id  INTEGER NOT NULL,
  org_risk_id     INTEGER NOT NULL,
  PRIMARY KEY (org_control_id, org_risk_id),
  FOREIGN KEY (org_control_id) REFERENCES org_controls(id) ON DELETE CASCADE,
  FOREIGN KEY (org_risk_id)    REFERENCES org_risks(id)    ON DELETE CASCADE
);

-- ===============================
-- Site risks & controls
-- ===============================
CREATE TABLE IF NOT EXISTS site_risks (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id       INTEGER NOT NULL,
  code          TEXT,
  title         TEXT NOT NULL,
  description   TEXT,
  status        TEXT,
  severity      TEXT,
  category      TEXT,
  owner_name    TEXT,
  owner_email   TEXT,
  created_at    TEXT DEFAULT (datetime('now')),
  updated_at    TEXT,
  FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_site_risks_site_id   ON site_risks(site_id);
CREATE INDEX IF NOT EXISTS idx_site_risks_status    ON site_risks(status);
CREATE INDEX IF NOT EXISTS idx_site_risks_severity  ON site_risks(severity);
CREATE INDEX IF NOT EXISTS idx_site_risks_category  ON site_risks(category);

CREATE TABLE IF NOT EXISTS site_controls (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id       INTEGER NOT NULL,
  code          TEXT,
  title         TEXT NOT NULL,
  description   TEXT,
  owner_name    TEXT,
  owner_email   TEXT,
  status        TEXT,
  created_at    TEXT DEFAULT (datetime('now')),
  updated_at    TEXT,
  FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_site_controls_site_id ON site_controls(site_id);

-- Link table: site controls ↔ site risks (many-to-many)
CREATE TABLE IF NOT EXISTS site_controls_risks (
  site_control_id INTEGER NOT NULL,
  site_risk_id    INTEGER NOT NULL,
  PRIMARY KEY (site_control_id, site_risk_id),
  FOREIGN KEY (site_control_id) REFERENCES site_controls(id) ON DELETE CASCADE,
  FOREIGN KEY (site_risk_id)    REFERENCES site_risks(id)    ON DELETE CASCADE
);

-- ===============================
-- Saved items & folders (align with your current code)
-- ===============================
CREATE TABLE IF NOT EXISTS folders (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  user_email   TEXT NOT NULL,
  name         TEXT NOT NULL,
  created_at   TEXT DEFAULT (datetime('now')),
  UNIQUE(user_email, name)
);

CREATE TABLE IF NOT EXISTS saved_items (
  user_email   TEXT NOT NULL,
  item_guid    TEXT NOT NULL,
  folder_id    INTEGER,
  created_at   TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (user_email, item_guid),
  FOREIGN KEY (folder_id) REFERENCES folders(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_saved_items_folder_id ON saved_items(folder_id);

-- Optional: tags from item → org/site/control (if you use this feature)
CREATE TABLE IF NOT EXISTS user_item_tags (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  user_email     TEXT NOT NULL,
  item_guid      TEXT NOT NULL,
  org_id         INTEGER NOT NULL,
  site_id        INTEGER,       -- nullable for Corporate tag
  org_control_id INTEGER,       -- nullable
  created_at     TEXT DEFAULT (datetime('now'))
);
-- Note: uniqueness with NULLs in SQLite is tricky. We rely on app code to prevent dup rows.
CREATE INDEX IF NOT EXISTS idx_user_item_tags_key
  ON user_item_tags(user_email, item_guid, org_id, site_id, org_control_id);

CREATE INDEX IF NOT EXISTS idx_org_risks_org_status_sev_cat
  ON org_risks(org_id, status, severity, category);

CREATE INDEX IF NOT EXISTS idx_site_risks_site_status_sev_cat
  ON site_risks(site_id, status, severity, category);



