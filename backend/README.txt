SWAG Order Portal - Fixed Backend

Service root: backend/
Start command: uvicorn main:app --host 0.0.0.0 --port $PORT

Required environment variables are documented at the top of main.py.
Keep the branch_partner_map.json file persistent when deploying so verified
branch/customer mappings survive restarts.
