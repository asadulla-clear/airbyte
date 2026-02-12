#!/usr/bin/env python3
"""
Generate and manage PRISM_TOKEN with automatic .env persistence.

This unified utility (replaces both scripts/generate_token.py and prism_client/generate_token.py):
1. Checks if PRISM_TOKEN exists (environment variable → .env file)
2. If not found, generates a new caller source token via REST API  
3. Automatically saves token to .env file
4. Can be used as CLI tool or imported as module

Usage:
    # As CLI tool:
    python scripts/generate_token.py
    python scripts/generate_token.py --force  # Force new token
    python scripts/generate_token.py --name my-app --namespaces analytics

    # As module:
    from generate_token import get_or_create_token, get_config
    token = get_or_create_token()
    host = get_config('PRISM_TEST_HTTP_HOST', 'localhost')
"""

import os
import sys
import time
import argparse
import requests
from pathlib import Path

# Path to .env file (looks in current working directory, then fallback to PRISM_HOME env var)
def get_env_file_path():
    """Get .env file path - checks current directory first, then PRISM_HOME."""
    # Check current working directory
    cwd_env = Path.cwd() / ".env"
    if cwd_env.exists():
        return cwd_env

    # Check PRISM_HOME environment variable
    prism_home = os.getenv('PRISM_HOME')
    if prism_home:
        prism_env = Path(prism_home) / ".env"
        if prism_env.exists():
            return prism_env

    # Default to current working directory (will be created if needed)
    return cwd_env

ENV_FILE = get_env_file_path()


def read_env_file():
    """Read .env file and return as dict."""
    env_vars = {}

    if not ENV_FILE.exists():
        return env_vars

    with open(ENV_FILE, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue

            # Parse KEY=VALUE
            if '=' in line:
                key, value = line.split('=', 1)
                env_vars[key.strip()] = value.strip()

    return env_vars


def get_config(key, default=None):
    """
    Get configuration value from environment variable or .env file.

    Priority: os.environ > .env file > default

    Args:
        key: Configuration key (e.g., 'PRISM_TEST_HTTP_HOST')
        default: Default value if not found

    Returns:
        Configuration value or default
    """
    # Check environment variable first
    value = os.getenv(key)
    if value is not None:
        return value

    # Check .env file
    env_vars = read_env_file()
    return env_vars.get(key, default)


def parse_prism_server_url(server_url: str) -> tuple[str, int, bool]:
    """
    Parse Prism server URL into host, port, and TLS flag.

    Args:
        server_url: Server URL (e.g., 'grpc://localhost:8815' or 'grpc+tls://host:port')

    Returns:
        Tuple of (host, port, use_tls)

    Example:
        >>> parse_prism_server_url('grpc://localhost:8815')
        ('localhost', 8815, False)
        >>> parse_prism_server_url('grpc+tls://prism.example.com:443')
        ('prism.example.com', 443, True)
    """
    use_tls = server_url.startswith('grpc+tls://')
    clean_url = server_url.replace('grpc+tls://', '').replace('grpc://', '')

    if ':' in clean_url:
        host, port_str = clean_url.split(':', 1)
        port = int(port_str)
    else:
        host = clean_url
        port = int(get_config('PRISM_SERVER_PORT', '8815'))

    return host, port, use_tls


def write_env_file(env_vars):
    """
    Write env_vars dict to .env file, preserving existing content.

    Only updates PRISM_TOKEN line, preserves all other configuration.
    """
    # Read existing file content
    existing_lines = []
    token_found = False

    if ENV_FILE.exists():
        with open(ENV_FILE, 'r') as f:
            for line in f:
                stripped = line.strip()
                # Skip existing PRISM_TOKEN line
                if stripped.startswith('PRISM_TOKEN='):
                    token_found = True
                    continue
                existing_lines.append(line.rstrip('\n'))

    # Write back with updated token
    with open(ENV_FILE, 'w') as f:
        # Write existing content
        for line in existing_lines:
            f.write(f"{line}\n")

        # Add token (with comment if it's new)
        if not token_found:
            f.write("\n# Auto-generated PRISM_TOKEN for test scripts\n")

        f.write(f"PRISM_TOKEN={env_vars['PRISM_TOKEN']}\n")


def create_caller_source_token(name=None, scopes=None, allowed_namespaces=None):
    """
    Create a new caller source token via REST API.

    Args:
        name: Optional caller source name (default: prism-test-<timestamp>)
        scopes: List of scopes (default: ["read", "write"])
        allowed_namespaces: List of allowed namespaces (default: ["*"])

    Returns:
        Dict with caller source details including token, or None if failed
    """
    if name is None:
        name = f"prism-test-{int(time.time())}"
    if scopes is None:
        scopes = ["read", "write"]
    if allowed_namespaces is None:
        allowed_namespaces = ["*"]

    # Get HTTP API configuration from .env
    http_host = get_config('PRISM_TEST_HTTP_HOST', 'localhost')
    http_port = get_config('PRISM_TEST_HTTP_PORT', '9815')
    base_url = f"http://{http_host}:{http_port}"
    api_url = f"{base_url}/api/v1/caller-sources/"

    payload = {
        "name": name,
        "scopes": scopes,
        "allowed_namespaces": allowed_namespaces
    }

    try:
        print(f"🔧 Creating caller source: {name}")
        response = requests.post(api_url, json=payload, timeout=5)

        if response.status_code == 201:
            data = response.json()
            print(f"✅ Caller source created successfully")
            print(f"   Token: {data['token'][:30]}...")
            return data

        # If caller source already exists (409), regenerate token for the existing one
        elif response.status_code == 409:
            print(f"⚠️  Caller source '{name}' already exists, regenerating token...")
            regenerate_url = f"{base_url}/api/v1/caller-sources/regenerate/{name}"
            try:
                regen_response = requests.post(regenerate_url, timeout=5)
                if regen_response.status_code == 200:
                    data = regen_response.json()
                    print(f"✅ Token regenerated successfully")
                    print(f"   Token: {data['token'][:30]}...")
                    return data
                else:
                    print(f"❌ Failed to regenerate token: {regen_response.status_code}")
                    print(f"   Response: {regen_response.text[:200]}")
                    return None
            except Exception as e:
                print(f"❌ Error regenerating token: {e}")
                return None

        else:
            print(f"❌ Failed to create caller source: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return None

    except requests.exceptions.ConnectionError:
        print(f"❌ Connection failed: Is Prism server running at {base_url}?")
        return None
    except Exception as e:
        print(f"❌ Error creating caller source: {e}")
        return None


def get_or_create_token(force_new=False, name=None, scopes=None, allowed_namespaces=None):
    """
    Get existing PRISM_TOKEN from environment/.env or create a new one.

    This is the main entry point for scripts. It will:
    1. Check environment variable PRISM_TOKEN
    2. Check .env file for PRISM_TOKEN
    3. If not found (or force_new=True), create new token via API
    4. Save new token to .env file
    5. Set in os.environ for current session

    Args:
        force_new: If True, always create a new token
        name: Optional caller source name (for new tokens)
        scopes: Optional scopes list (for new tokens)
        allowed_namespaces: Optional namespaces list (for new tokens)

    Returns:
        Token string or None if failed
    """
    # First, check environment variable (takes precedence)
    if not force_new:
        env_token = os.getenv('PRISM_TOKEN')
        if env_token:
            print(f"📝 Using PRISM_TOKEN from environment: {env_token[:30]}...")
            return env_token

    # Read .env file
    env_vars = read_env_file()

    # Check if token exists in .env
    if not force_new and 'PRISM_TOKEN' in env_vars:
        token = env_vars['PRISM_TOKEN']
        print(f"📝 Using PRISM_TOKEN from .env: {token[:30]}...")
        # Set in environment for current session
        os.environ['PRISM_TOKEN'] = token
        return token

    # No token found or force_new - create a new one
    print("📝 No PRISM_TOKEN found, generating new token...")
    print()

    caller_source = create_caller_source_token(
        name=name,
        scopes=scopes,
        allowed_namespaces=allowed_namespaces
    )

    if not caller_source:
        print("\n❌ ERROR: Failed to generate token!")
        http_host = get_config('PRISM_TEST_HTTP_HOST', 'localhost')
        http_port = get_config('PRISM_TEST_HTTP_PORT', '9815')
        print(f"Make sure Prism server is running at http://{http_host}:{http_port}")
        return None

    token = caller_source['token']

    # Save to .env
    env_vars['PRISM_TOKEN'] = token
    write_env_file(env_vars)

    print()
    print(f"💾 Token saved to: {ENV_FILE}")
    print(f"   You can now use: export PRISM_TOKEN={token}")
    print()

    # Set in environment for current session
    os.environ['PRISM_TOKEN'] = token

    return token


def main():
    """Command-line interface with detailed output."""
    parser = argparse.ArgumentParser(
        description='Generate and manage PRISM caller source tokens',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Get existing token or create new one
  python scripts/generate_token.py

  # Force create a new token
  python scripts/generate_token.py --force

  # Create token for specific app
  python scripts/generate_token.py --name my-analytics-app

  # Create token with specific namespaces
  python scripts/generate_token.py --name my-app --namespaces analytics reporting

  # Create read-only token
  python scripts/generate_token.py --name readonly-client --scopes read
        """
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force create a new token even if one exists'
    )

    parser.add_argument(
        '--name',
        type=str,
        help='Custom caller source name (default: prism-test-<timestamp>)'
    )

    parser.add_argument(
        '--scopes',
        nargs='+',
        default=['read', 'write'],
        help='Scopes for the token (default: read write)'
    )

    parser.add_argument(
        '--namespaces',
        nargs='+',
        default=['*'],
        help='Allowed namespaces (default: * for all)'
    )

    args = parser.parse_args()

    print("=" * 70)
    print("PRISM TOKEN GENERATOR")
    print("=" * 70)
    print()

    if args.force:
        print("🔄 Force creating new token...")

    token = get_or_create_token(
        force_new=args.force,
        name=args.name,
        scopes=args.scopes,
        allowed_namespaces=args.namespaces
    )

    if not token:
        sys.exit(1)

    # Show usage examples
    print("=" * 70)
    print("💡 Usage Examples:")
    print("=" * 70)

    print("\n1️⃣  Python (PrismClient):")
    print(f"""
from prism_client import PrismClient

client = PrismClient(
    host="localhost",
    port=8815,
    token="{token}",
    tenant_id="your-tenant-id"
)

result = client.query("SELECT 1 as test")
print(result.to_pandas())
    """)

    print("\n2️⃣  Environment Variable:")
    print(f"""
export PRISM_TOKEN="{token}"
export PRISM_TENANT_ID="your-tenant-id"

# Then in Python:
import os
from prism_client import PrismClient

client = PrismClient(
    host="localhost",
    port=8815,
    token=os.getenv("PRISM_TOKEN"),
    tenant_id=os.getenv("PRISM_TENANT_ID")
)
    """)

    print("\n3️⃣  Flight SQL (PyArrow):")
    print(f"""
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")
headers = [
    (b"authorization", b"Bearer {token}"),
    (b"x-tenant-id", b"your-tenant-id")
]

action = flight.Action("DescribeTable", b"your_table")
results = list(client.do_action(action, flight.FlightCallOptions(headers=headers)))
    """)

    print("\n" + "=" * 70)
    sys.exit(0)


if __name__ == "__main__":
    main()
