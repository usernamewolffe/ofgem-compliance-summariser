-- 1) Folders (user-private)
CREATE TABLE IF NOT EXISTS folders (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  user_email  TEXT NOT NULL,
  name        TEXT NOT NULL,
  created_at  TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_folders_user_name
  ON folders(user_email, name);

-- 2) Saved items (bookmarks per user)
CREATE TABLE IF NOT EXISTS saved_items (
  user_email  TEXT NOT NULL,
  item_guid   TEXT NOT NULL,
  folder_id   INTEGER,
  note        TEXT,
  created_at  TEXT NOT NULL,
  PRIMARY KEY (user_email, item_guid),
  FOREIGN KEY (folder_id) REFERENCES folders(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_saved_items_folder
  ON saved_items(folder_id);

-- 3) User tags (private links items → sites/controls)
CREATE TABLE IF NOT EXISTS user_item_tags (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  user_email      TEXT NOT NULL,
  item_guid       TEXT NOT NULL,
  org_id          INTEGER NOT NULL,
  site_id         INTEGER,
  org_control_id  INTEGER,
  created_at      TEXT NOT NULL,
  FOREIGN KEY (site_id)        REFERENCES sites(id)         ON DELETE CASCADE,
  FOREIGN KEY (org_control_id) REFERENCES org_controls(id)  ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_u_tags_user_item
  ON user_item_tags(user_email, item_guid);
CREATE UNIQUE INDEX IF NOT EXISTS idx_u_tags_uniqueness
  ON user_item_tags(
    user_email,
    item_guid,
    IFNULL(site_id, -1),
    IFNULL(org_control_id, -1)
  );
