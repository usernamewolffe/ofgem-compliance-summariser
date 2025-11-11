# tools/map_org_controls.py
import argparse
from storage.db import DB

def main():
    ap = argparse.ArgumentParser(description="Map an org control to framework controls by ref.")
    ap.add_argument("--org-id", type=int, default=1, help="Organisation id (default 1)")
    ap.add_argument("--org-control", required=True,
                    help="Org control id OR code (e.g. 12 or 'IR-01')")
    ap.add_argument("--refs", nargs="+", required=True,
                    help="Framework control refs to map to (e.g. CAF-D1 A.5.1)")
    args = ap.parse_args()

    db = DB("ofgem.db")

    # Resolve org control id (by numeric id or by code)
    oc_id = None
    try:
        oc_id = int(args.org_control)
    except ValueError:
        for oc in db.list_org_controls(args.org_id):
            if (oc.get("code") or "").strip().lower() == args.org_control.strip().lower():
                oc_id = oc["id"]; break
    if not oc_id:
        raise SystemExit(f"Org control not found: {args.org_control}")

    # Resolve framework control ids by ref
    all_ctrls = db.list_controls()
    ref_to_id = {c["ref"]: c["id"] for c in all_ctrls}
    missing = [r for r in args.refs if r not in ref_to_id]
    if missing:
        raise SystemExit(f"Unknown control refs: {', '.join(missing)}")

    ctrl_ids = [ref_to_id[r] for r in args.refs]

    db.map_org_control_to_controls(oc_id, ctrl_ids)
    print(f"Mapped org_control {oc_id} to: {', '.join(args.refs)}")

if __name__ == "__main__":
    main()
