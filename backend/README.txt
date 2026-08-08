SWAG Order Portal - Fixed Backend v2

Fixes included:
- Login no longer fails when an optional warehouse field is restricted/missing in Odoo.
- Existing legacy branch_partner_map.json mappings are read and upgraded on next save.
- Branch-safe mapping and stale-session checks remain enabled.

Start:
  uvicorn main:app --host 0.0.0.0 --port $PORT

Required environment variables are documented at the top of main.py.
Keep branch_partner_map.json persistent in deployment.
