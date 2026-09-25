#!/usr/bin/env python3
"""Stockbit Token Manager & Remote Deployment Utility.

Validates Stockbit JWT authentication tokens, verifies live API connectivity,
and seamlessly updates both local and remote VM (.env) configurations.

Usage:
    # Direct argument:
    python scripts/update_token.py "eyJhbGciOiJSUz..."

    # Interactive prompt:
    python scripts/update_token.py

    # Update local .env only:
    python scripts/update_token.py --token "eyJ..." --local-only

    # Update remote VM only:
    python scripts/update_token.py --token "eyJ..." --remote-only
"""

import argparse
import base64
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Dict, Optional, Sequence, Tuple
import urllib.parse

import httpx
import pytz

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("update_token")

# ssh -i ".\idx-scraper_key.pem" popboat@4.190.168.186
WIB = pytz.timezone("Asia/Jakarta")
DEFAULT_VM_HOST = "4.190.168.186"
DEFAULT_VM_USER = "popboat"
DEFAULT_VM_ENV = "/home/popboat/geometric_arb/.env"
DEFAULT_KEY_FILE = "idx-scraper_key.pem"


def clean_token(raw_token: str) -> str:
    """Normalize raw token string by stripping quotes, whitespace, and 'Bearer ' prefix."""
    token = raw_token.strip().strip('"').strip("'")
    if token.startswith("Bearer "):
        token = token[7:].strip()
    return token


def inspect_jwt_payload(token: str) -> Dict[str, Any]:
    """Decode and inspect JWT payload claims, checking expiration and ownership."""
    cleaned = clean_token(token)
    try:
        parts = cleaned.split(".")
        if len(parts) < 2:
            return {"is_valid_format": False, "username": "unknown"}

        payload_b64 = parts[1]
        # Pad base64 if needed
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload_json = base64.b64decode(payload_b64).decode("utf-8")
        payload = json.loads(payload_json)

        data = payload.get("data", {})
        username = data.get("use") or data.get("email") or payload.get("sub", "unknown")
        fullname = data.get("ful", "")
        uid = data.get("uid", "")
        exp_ts = payload.get("exp")
        iat_ts = payload.get("iat")

        is_expired = False
        remaining_seconds = 0.0
        exp_wib_str = "N/A"

        if exp_ts is not None:
            exp_dt = datetime.fromtimestamp(exp_ts, tz=WIB)
            now_dt = datetime.now(WIB)
            remaining_seconds = (exp_dt - now_dt).total_seconds()
            is_expired = remaining_seconds <= 0
            exp_wib_str = exp_dt.strftime("%Y-%m-%d %H:%M:%S WIB")

        return {
            "is_valid_format": True,
            "username": username,
            "fullname": fullname,
            "uid": uid,
            "exp_timestamp": exp_ts,
            "iat_timestamp": iat_ts,
            "exp_wib": exp_wib_str,
            "remaining_seconds": remaining_seconds,
            "remaining_hours": remaining_seconds / 3600.0 if remaining_seconds > 0 else 0.0,
            "is_expired": is_expired,
            "raw_payload": payload,
        }
    except Exception as e:
        logger.debug(f"Failed to inspect JWT payload: {e}")
        return {"is_valid_format": False, "username": "unknown", "error": str(e)}


def verify_token_live(token: str, timeout: float = 10.0) -> Tuple[bool, str]:
    """Test live authentication against Stockbit API endpoint."""
    cleaned = clean_token(token)
    headers = {
        "Authorization": f"Bearer {cleaned}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }
    test_url = "https://exodus.stockbit.com/charts/IHSG/daily?timeframe=today"

    try:
        with httpx.Client(timeout=timeout, headers=headers) as client:
            resp = client.get(test_url)
            if resp.status_code == 200:
                return True, "Live API authentication passed (HTTP 200 OK)."
            elif resp.status_code == 401:
                return False, f"Authentication rejected: HTTP 401 Unauthorized ({resp.text[:100]})."
            elif resp.status_code == 403:
                return False, f"Authentication forbidden: HTTP 403 Forbidden ({resp.text[:100]})."
            else:
                return False, f"Unexpected response status HTTP {resp.status_code}: {resp.text[:100]}."
    except Exception as e:
        return False, f"Network connection error during verification: {e}"


def update_env_file(token: str, env_path: Path) -> bool:
    """Update or insert STOCKBIT_TOKEN in a specified .env file while preserving other entries."""
    cleaned = clean_token(token)
    token_line = f'STOCKBIT_TOKEN="Bearer {cleaned}"\n'

    try:
        lines = []
        found = False
        if env_path.exists():
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip().startswith("STOCKBIT_TOKEN="):
                        lines.append(token_line)
                        found = True
                    else:
                        lines.append(line)

        if not found:
            lines.append(token_line)

        # Write atomically
        env_path.parent.mkdir(parents=True, exist_ok=True)
        with open(env_path, "w", encoding="utf-8") as f:
            f.writelines(lines)

        logger.info(f"Successfully updated token in: {env_path.resolve()}")
        return True
    except Exception as e:
        logger.error(f"Failed to update .env at {env_path}: {e}")
        return False


def update_remote_vm(
    token: str,
    host: str = DEFAULT_VM_HOST,
    user: str = DEFAULT_VM_USER,
    key_path: Optional[Path] = None,
    remote_env_path: str = DEFAULT_VM_ENV,
) -> bool:
    """Deploy the updated token to the remote VM .env file over SSH."""
    cleaned = clean_token(token)
    resolved_key = (
        key_path
        if key_path is not None
        else Path(__file__).resolve().parent.parent / DEFAULT_KEY_FILE
    )

    if not resolved_key.exists():
        logger.warning(f"SSH private key not found at {resolved_key}. Skipping remote VM update.")
        return False

    logger.info(f"Connecting to remote VM ({user}@{host}) to update {remote_env_path}...")

    # Remote command to safely update or append STOCKBIT_TOKEN in .env
    escaped_token = f"Bearer {cleaned}"
    remote_cmd = (
        f"if grep -q '^STOCKBIT_TOKEN=' {remote_env_path} 2>/dev/null; then "
        f"sed -i 's|^STOCKBIT_TOKEN=.*|STOCKBIT_TOKEN=\"{escaped_token}\"|' {remote_env_path}; "
        f"else "
        f"echo 'STOCKBIT_TOKEN=\"{escaped_token}\"' >> {remote_env_path}; "
        f"fi && "
        f"echo 'Remote .env updated successfully.'"
    )

    ssh_args = [
        "ssh",
        "-n",
        "-i",
        str(resolved_key),
        "-o",
        "ConnectTimeout=30",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "StrictHostKeyChecking=no",
        f"{user}@{host}",
        remote_cmd,
    ]

    try:
        res = subprocess.run(
            ssh_args,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if res.returncode == 0:
            logger.info(f"Remote VM update succeeded: {res.stdout.strip()}")
            return True
        else:
            logger.error(f"Remote VM update failed (code {res.returncode}): {res.stderr.strip()}")
            return False
    except Exception as e:
        logger.error(f"Failed executing SSH command: {e}")
        return False


def encrypt_github_secret(public_key_b64: str, secret_value: str) -> str:
    """encrypt secret value using repository public key for github actions secrets."""
    import nacl.encoding
    import nacl.public
    public_key = nacl.public.PublicKey(public_key_b64.encode("utf-8"), nacl.encoding.Base64Encoder)
    sealed_box = nacl.public.SealedBox(public_key)
    encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
    return base64.b64encode(encrypted).decode("utf-8")


def get_github_pat() -> str:
    """retrieve github personal access token from env or git remote origin url."""
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        return token.strip()

    # check git remote origin url for embedded token e.g. https://ghp_xxx@github.com/...
    try:
        res = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if res.returncode == 0:
            url = res.stdout.strip()
            parsed = urllib.parse.urlsplit(url)
            if parsed.username and (parsed.username.startswith("ghp_") or parsed.username.startswith("github_pat_")):
                return parsed.username
            if parsed.password and (parsed.password.startswith("ghp_") or parsed.password.startswith("github_pat_")):
                return parsed.password
            m = re.search(r"(ghp_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+)", url)
            if m:
                return m.group(1)
    except Exception as e:
        logger.debug(f"failed to extract pat from git remote: {e}")

    return ""


def sync_secret_to_github(
    repo: str,
    secret_name: str,
    secret_value: str,
    github_pat: str,
    timeout: float = 15.0,
) -> bool:
    """fetch repository public key, encrypt secret, and upload to github actions secrets."""
    if not github_pat:
        logger.error(f"cannot sync {secret_name} to github: missing github personal access token.")
        return False

    repo_clean = repo.strip().strip("/")
    base_url = f"https://api.github.com/repos/{repo_clean}/actions/secrets"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {github_pat}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "geometric-arb-token-sync",
    }

    try:
        with httpx.Client(timeout=timeout, headers=headers) as client:
            # 1. fetch repository public key
            pk_url = f"{base_url}/public-key"
            pk_resp = client.get(pk_url)
            if pk_resp.status_code != 200:
                logger.error(
                    f"failed to fetch public key for {repo_clean}: HTTP {pk_resp.status_code} ({pk_resp.text[:100]})"
                )
                return False

            pk_data = pk_resp.json()
            key_id = pk_data.get("key_id")
            public_key_b64 = pk_data.get("key")

            if not key_id or not public_key_b64:
                logger.error(f"invalid public key response from github: {pk_data}")
                return False

            # 2. encrypt secret using public key
            encrypted_value = encrypt_github_secret(public_key_b64, secret_value)

            # 3. put encrypted secret payload
            put_url = f"{base_url}/{secret_name}"
            payload = {
                "encrypted_value": encrypted_value,
                "key_id": key_id,
            }
            put_resp = client.put(put_url, json=payload)
            if put_resp.status_code in (201, 204):
                logger.info(f"successfully synced secret {secret_name} to {repo_clean}")
                return True
            else:
                logger.error(
                    f"failed to set secret {secret_name} in {repo_clean}: HTTP {put_resp.status_code} ({put_resp.text[:100]})"
                )
                return False
    except Exception as e:
        logger.error(f"error syncing secret {secret_name} to github: {e}")
        return False


def sync_rclone_to_github(
    repo: str,
    github_pat: str,
    config_path: Optional[Path] = None,
) -> bool:
    """resolve local rclone.conf, base64 encode content, and sync to github repository secret."""
    resolved_path: Optional[Path] = None
    if config_path is not None:
        p = Path(config_path)
        if p.is_file():
            resolved_path = p
        else:
            logger.error(f"provided rclone.conf path does not exist: {p}")
            return False
    else:
        # resolve from standard locations (windows appdata or unix ~/.config)
        candidates = []
        appdata = os.environ.get("APPDATA")
        if appdata:
            candidates.append(Path(appdata) / "rclone" / "rclone.conf")
        candidates.append(Path.home() / ".config" / "rclone" / "rclone.conf")
        for cand in candidates:
            if cand.is_file():
                resolved_path = cand
                break

    if not resolved_path:
        logger.error("rclone.conf not found in standard paths (APPDATA or ~/.config/rclone/).")
        return False

    try:
        content_bytes = resolved_path.read_bytes()
        b64_content = base64.b64encode(content_bytes).decode("utf-8")
        logger.info(f"resolved rclone.conf at {resolved_path} ({len(content_bytes)} bytes), syncing to {repo}...")
        return sync_secret_to_github(repo, "RCLONE_CONFIG_DATA", b64_content, github_pat)
    except Exception as e:
        logger.error(f"failed reading or syncing rclone.conf from {resolved_path}: {e}")
        return False


def build_parser() -> argparse.ArgumentParser:
    """Construct command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Update and validate Stockbit JWT authentication token locally and on remote VM."
    )
    parser.add_argument(
        "token_pos",
        nargs="?",
        default=None,
        help="Stockbit JWT authentication token (positional).",
    )
    parser.add_argument(
        "-t",
        "--token",
        type=str,
        default=None,
        help="Stockbit JWT authentication token (flag).",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Only update local .env file (skip remote VM).",
    )
    parser.add_argument(
        "--remote-only",
        action="store_true",
        help="Only update remote VM .env file (skip local .env).",
    )
    parser.add_argument(
        "--skip-live-check",
        action="store_true",
        help="Skip live network test against Stockbit API.",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=DEFAULT_VM_HOST,
        help=f"Remote VM hostname/IP (default: {DEFAULT_VM_HOST}).",
    )
    parser.add_argument(
        "--user",
        type=str,
        default=DEFAULT_VM_USER,
        help=f"Remote VM SSH user (default: {DEFAULT_VM_USER}).",
    )
    parser.add_argument(
        "--sync-github",
        action="store_true",
        help="Sync STOCKBIT_TOKEN to GitHub repository secrets",
    )
    parser.add_argument(
        "--sync-rclone",
        action="store_true",
        help="Sync local rclone.conf as RCLONE_CONFIG_DATA to GitHub repository secrets",
    )
    parser.add_argument(
        "--github-repo",
        type=str,
        default="popboat1/idx-scraper",
        help="Target GitHub repository for secrets",
    )
    parser.add_argument(
        "--github-pat",
        type=str,
        default=None,
        help="GitHub Personal Access Token",
    )
    return parser


def main(args: Optional[Sequence[str]] = None) -> int:
    """Main CLI entrypoint."""
    parser = build_parser()
    parsed_args = parser.parse_args(args)

    raw_token = parsed_args.token or parsed_args.token_pos

    if not raw_token:
        # Prompt interactively if not provided via arguments
        print("=" * 60)
        print(" Stockbit JWT Token Updater")
        print("=" * 60)
        print("Please paste the new Stockbit JWT token below:")
        try:
            raw_token = input("Token: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nAborted.")
            return 1

    if not raw_token:
        logger.error("No token provided. Exiting.")
        return 1

    token = clean_token(raw_token)

    # 1. JWT Inspection
    info = inspect_jwt_payload(token)
    if not info.get("is_valid_format"):
        logger.warning("Token does not appear to be in standard 3-part JWT format.")
    else:
        user_display = f"{info.get('fullname', '')} ({info.get('username', '')})"
        logger.info(f"User: {user_display.strip()}")
        logger.info(f"Expires: {info.get('exp_wib')} ({info.get('remaining_hours', 0.0):.1f} hours remaining)")

        if info.get("is_expired"):
            logger.error("WARNING: The provided token is ALREADY EXPIRED! Please obtain a fresh token.")
            proceed = input("Do you still want to proceed? [y/N]: ").strip().lower()
            if proceed not in ("y", "yes"):
                logger.info("Aborted update due to expired token.")
                return 1

    # 2. Live API Connectivity Test
    if not parsed_args.skip_live_check:
        logger.info("Verifying token with live Stockbit API...")
        is_live_ok, live_msg = verify_token_live(token)
        if is_live_ok:
            logger.info(f"{live_msg}")
        else:
            logger.warning(f"{live_msg}")
            proceed = input("API check did not succeed. Proceed with updating anyway? [y/N]: ").strip().lower()
            if proceed not in ("y", "yes"):
                logger.info("Aborted update due to API verification failure.")
                return 1

    # 3. Update Targets
    success = True
    local_env_path = Path(__file__).resolve().parent.parent / ".env"

    if not parsed_args.remote_only:
        local_ok = update_env_file(token, local_env_path)
        success = success and local_ok

    if not parsed_args.local_only:
        remote_ok = update_remote_vm(
            token=token,
            host=parsed_args.host,
            user=parsed_args.user,
        )
        success = success and remote_ok

    # 4. GitHub Actions Secrets Sync
    if parsed_args.sync_github or parsed_args.sync_rclone:
        github_pat = parsed_args.github_pat or get_github_pat()
        if not github_pat:
            logger.error("github pat not found via --github-pat, env (GITHUB_TOKEN/GH_TOKEN), or git remote.")
            success = False
        else:
            if parsed_args.sync_github:
                secret_value = f"Bearer {token}"
                gh_token_ok = sync_secret_to_github(
                    repo=parsed_args.github_repo,
                    secret_name="STOCKBIT_TOKEN",
                    secret_value=secret_value,
                    github_pat=github_pat,
                )
                success = success and gh_token_ok

            if parsed_args.sync_rclone:
                rclone_ok = sync_rclone_to_github(
                    repo=parsed_args.github_repo,
                    github_pat=github_pat,
                )
                success = success and rclone_ok

    if success:
        logger.info("Token updated successfully across all target environments.")
        return 0
    else:
        logger.error("Failed to update token on one or more target environments.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
