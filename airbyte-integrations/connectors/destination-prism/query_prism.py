import json
import sys
import os
from destination_prism.prism_client import PrismClient

def main():
    if len(sys.argv) < 2:
        print('Usage: python3 query_prism.py "SELECT * FROM users" [execution_engine]')
        print('Engines: trino (default), duckdb')
        sys.exit(1)

    query = sys.argv[1]
    engine = sys.argv[2] if len(sys.argv) > 2 else "trino"
    config_path = "integration_tests/config.json"

    if not os.path.exists(config_path):
        print(f"Error: {config_path} not found.")
        sys.exit(1)

    # Load config
    with open(config_path, "r") as f:
        config = json.load(f)

    print(f"Connecting to Prism at {config['grpc_url']}...")
    client = PrismClient(
        grpc_url=config["grpc_url"],
        http_url=config["http_url"],
        token=config["token"],
        tenant_id=config["tenant_id"]
    )

    try:
        print(f"Executing query: {query} (engine: {engine})")
        # returns results as list of dicts
        results = client.query(query, return_type="dict", execution_engine=engine)
        
        if not results:
            print("Query returned no results.")
            return

        # Simple table formatting
        headers = list(results[0].keys())
        print("\n" + " | ".join(headers))
        print("-" * (len(" | ".join(headers)) + 4))
        
        for row in results:
            print(" | ".join(str(row.get(h, "")) for h in headers))
            
        print(f"\nTotal rows: {len(results)}")

    except Exception as e:
        print(f"\nError executing query: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    main()
