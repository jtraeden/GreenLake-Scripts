#!/usr/bin/env python3
"""
HPE GreenLake Platform (GLCP) - Bulk Device Subscription Swap
--------------------------------------------------------------
Reads serial numbers from a plain-text file (one per line), removes the
current subscription from each device, and applies a new subscription using
the official GLCP public REST API.

Official API docs:
  https://developer.greenlake.hpe.com/docs/greenlake/services/device-management/public/guide/
  https://developer.greenlake.hpe.com/docs/greenlake/services/subscription-management/public/guide/

Endpoints used:
  GET   /devices/v1/devices?filter=serialNumber eq '{sn}'
  GET   /subscriptions/v1/subscriptions?filter=key eq '{sub_key}'
  PATCH /devices/v1/devices?id={device_id}   (Content-Type: application/merge-patch+json)

Usage:
    python greenlake_swap_subscriptions.py \
        --serials serials.txt \
        --new-subscription-key SUB-XXXX-XXXX \
        --client-id YOUR_CLIENT_ID \
        --client-secret YOUR_CLIENT_SECRET \
        [--dry-run]

Requirements:
    pip install requests
"""

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GLCP_TOKEN_URL = "https://sso.common.cloud.hpe.com/as/token.oauth2"
GLCP_API_BASE  = "https://global.api.greenlake.hpe.com"

# Seconds between device API calls to avoid hitting rate limits
REQUEST_DELAY = 1 

# Seconds to wait after a successful unassign before attempting to assign.
# GLCP needs time to settle the device state before accepting a new subscription.
POST_REMOVE_SETTLE = 2

# Async polling settings
ASYNC_POLL_INTERVAL = 5
ASYNC_POLL_TIMEOUT  = 600   # 10 minutes – subscription ops can be slow
# ---------------------------------------------------------------------------


def setup_logging(log_file: str) -> logging.Logger:
    logger = logging.getLogger("glcp_swap")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------
def get_access_token(client_id: str, client_secret: str,
                     logger: logging.Logger) -> tuple[str, float]:
    """
    Returns (access_token, expiry_timestamp).
    expiry_timestamp is time.time() + expires_in - 60s safety buffer.
    """
    logger.info("Requesting OAuth2 access token ...")
    resp = requests.post(
        GLCP_TOKEN_URL,
        data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    data  = resp.json()
    token = data.get("access_token")
    if not token:
        raise ValueError("No access_token in response: " + resp.text)
    expires_in  = data.get("expires_in", 7200)
    expiry_time = time.time() + expires_in - 60   # refresh 60s before expiry
    logger.info("Access token obtained. Expires in %ds.", expires_in)
    return token, expiry_time


# ---------------------------------------------------------------------------
# Generic HTTP helpers
# ---------------------------------------------------------------------------
def api_get(session: requests.Session, url: str,
            params: dict | None, logger: logging.Logger) -> dict:
    resp = session.get(url, params=params, timeout=30)
    logger.debug("GET %s params=%s -> %s", url, params, resp.status_code)
    resp.raise_for_status()
    return resp.json()


def api_patch(session: requests.Session, url: str, payload: dict,
              params: dict | None, logger: logging.Logger) -> requests.Response:
    # CRITICAL: GLCP PATCH endpoints require 'application/merge-patch+json'.
    # Using 'application/json' causes the server to return 202 but silently
    # ignore the body, so the subscription is never actually applied and the
    # async operation hangs until timeout.
    patch_headers = {"Content-Type": "application/merge-patch+json"}
    resp = session.patch(url, json=payload, params=params,
                         headers=patch_headers, timeout=30)
    logger.debug("PATCH %s params=%s body=%s -> %s",
                 url, params, json.dumps(payload), resp.status_code)
    resp.raise_for_status()
    return resp


# ---------------------------------------------------------------------------
# Async operation polling
# ---------------------------------------------------------------------------
def poll_async(session: requests.Session, operation_uri: str,
               logger: logging.Logger) -> bool:
    """
    Poll a GLCP async operation until success, failure, or timeout.
    Handles both absolute URLs and relative paths from the Location header.
    Uses the API's own suggestedPollingIntervalSeconds when provided.
    """
    url = operation_uri if operation_uri.startswith("http") \
          else GLCP_API_BASE + operation_uri

    logger.info("  Polling: %s", url)
    deadline      = time.time() + ASYNC_POLL_TIMEOUT
    attempt       = 0
    elapsed_start = time.time()

    while time.time() < deadline:
        try:
            data = api_get(session, url, None, logger)
        except requests.HTTPError as exc:
            logger.error("Async poll error: %s", exc)
            return False

        attempt  += 1
        status    = data.get("status", "UNKNOWN").upper()
        progress  = data.get("progressPercent", "?")
        elapsed   = int(time.time() - elapsed_start)
        poll_wait = max(1, int(data.get("suggestedPollingIntervalSeconds",
                                        ASYNC_POLL_INTERVAL)))

        logger.info("  [%3ds elapsed | attempt %d] status=%-12s progress=%s%%"
                    "  (next poll in %ds)",
                    elapsed, attempt, status, progress, poll_wait)

        # Terminal states per GLCP API spec: SUCCEEDED, FAILED, TIMEOUT
        if status == "SUCCEEDED":
            return True
        if status in ("FAILED", "TIMEOUT"):
            logger.error("Async operation failed. Status: %s  Result: %s",
                         status, data.get("result"))
            return False
        # In-progress states are INITIALIZED and RUNNING – keep polling

        time.sleep(poll_wait)

    logger.error("Async poll timed out after %ds for: %s",
                 ASYNC_POLL_TIMEOUT, url)
    return False


def get_op_uri(resp: requests.Response, logger: logging.Logger,
               serial: str) -> str:
    """
    Extract the async operation URI from a 202 response.
    Checks the Location header first, then falls back to the response body.
    """
    op_uri = resp.headers.get("Location", "")
    if not op_uri:
        try:
            body   = resp.json()
            op_uri = body.get("location", body.get("operationUri", ""))
        except ValueError:
            pass
    if not op_uri:
        logger.warning("Serial %s – 202 received but no Location URI found; "
                       "cannot confirm operation completed.", serial)
    return op_uri


# ---------------------------------------------------------------------------
# Device & subscription lookups
# ---------------------------------------------------------------------------
def get_device_by_serial(session: requests.Session, serial: str,
                         logger: logging.Logger) -> dict | None:
    """GET /devices/v1/devices?filter=serialNumber eq '<serial>'"""
    url    = f"{GLCP_API_BASE}/devices/v1/devices"
    params = {"filter": f"serialNumber eq '{serial}'"}
    try:
        data = api_get(session, url, params, logger)
    except requests.HTTPError as exc:
        logger.error("Serial %s – device lookup failed: %s", serial, exc)
        return None

    items = data.get("items", [])
    if not items:
        logger.warning("Serial %s – device NOT FOUND in workspace.", serial)
        return None
    if len(items) > 1:
        logger.warning("Serial %s – %d matches found; using first.",
                       serial, len(items))
    return items[0]


def get_subscription_id_by_key(session: requests.Session, sub_key: str,
                                logger: logging.Logger) -> str | None:
    """
    Resolve a subscription key (e.g. SUB-XXXX-XXXX) to its internal UUID.
    GET /subscriptions/v1/subscriptions?filter=key eq '<sub_key>'
    """
    url    = f"{GLCP_API_BASE}/subscriptions/v1/subscriptions"
    params = {"filter": f"key eq '{sub_key}'"}
    try:
        data = api_get(session, url, params, logger)
    except requests.HTTPError as exc:
        logger.error("Subscription lookup failed for key '%s': %s", sub_key, exc)
        return None

    items = data.get("items", [])
    if not items:
        logger.error("Subscription key '%s' NOT FOUND in workspace.", sub_key)
        return None

    sub_id = items[0].get("id")
    logger.info("Subscription key '%s' resolved to ID: %s", sub_key, sub_id)
    return sub_id


# ---------------------------------------------------------------------------
# Subscription operations
# ---------------------------------------------------------------------------
def remove_subscription(session: requests.Session, device: dict,
                        logger: logging.Logger, dry_run: bool) -> bool:
    """
    Remove all subscriptions from a device.
    PATCH /devices/v1/devices?id={device_id}  body: {"subscription": []}
    """
    serial    = device.get("serialNumber", "?")
    device_id = device.get("id")

    current_subs = device.get("subscription") or []
    if not current_subs:
        logger.info("Serial %s – no subscription currently assigned; "
                    "skipping removal.", serial)
        return True

    current_keys = [s.get("key", s.get("id", "?")) for s in current_subs]
    logger.info("Serial %s – removing subscription(s): %s",
                serial, ", ".join(current_keys))

    if dry_run:
        logger.info("[DRY-RUN] Serial %s – would remove subscription(s).", serial)
        return True

    url     = f"{GLCP_API_BASE}/devices/v1/devices"
    payload = {"subscription": []}
    params  = {"id": device_id}

    try:
        resp = api_patch(session, url, payload, params, logger)
    except requests.HTTPError as exc:
        logger.error("Serial %s – remove subscription request failed: %s",
                     serial, exc)
        return False

    if resp.status_code == 202:
        op_uri = get_op_uri(resp, logger, serial)
        if op_uri:
            logger.info("Serial %s – waiting for async remove operation ...",
                        serial)
            if not poll_async(session, op_uri, logger):
                logger.error("Serial %s – async remove operation failed.", serial)
                return False

    logger.info("Serial %s – subscription(s) removed. "
                "Waiting %ds for device state to settle ...",
                serial, POST_REMOVE_SETTLE)
    time.sleep(POST_REMOVE_SETTLE)
    return True


def apply_subscription(session: requests.Session, device: dict,
                       sub_id: str, sub_key: str,
                       logger: logging.Logger, dry_run: bool) -> bool:
    """
    Assign a subscription to a device.
    PATCH /devices/v1/devices?id={device_id}  body: {"subscription": [{"id": sub_id}]}
    Content-Type must be application/merge-patch+json (enforced in api_patch).
    """
    serial    = device.get("serialNumber", "?")
    device_id = device.get("id")

    if dry_run:
        logger.info("[DRY-RUN] Serial %s – would apply subscription '%s'.",
                    serial, sub_key)
        return True

    url     = f"{GLCP_API_BASE}/devices/v1/devices"
    payload = {"subscription": [{"id": sub_id}]}
    params  = {"id": device_id}

    try:
        resp = api_patch(session, url, payload, params, logger)
    except requests.HTTPError as exc:
        logger.error("Serial %s – apply subscription request failed: %s",
                     serial, exc)
        return False

    if resp.status_code == 202:
        op_uri = get_op_uri(resp, logger, serial)
        if op_uri:
            logger.info("Serial %s – waiting for async apply operation ...",
                        serial)
            if not poll_async(session, op_uri, logger):
                logger.error("Serial %s – async apply operation failed.", serial)
                return False

    logger.info("Serial %s – subscription '%s' applied successfully.",
                serial, sub_key)
    return True


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------
def load_serials(file_path: str) -> list[str]:
    serials = []
    with open(file_path, encoding="utf-8") as fh:
        for line in fh:
            val = line.strip()
            if val and not val.startswith("#"):
                serials.append(val)
    return serials


def write_results(results: list[dict], out_path: str):
    fields = ["serial_number", "device_id", "old_subscription",
              "new_subscription", "status", "error"]
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Swap HPE GreenLake device subscriptions in bulk."
    )
    parser.add_argument("--serials", required=True,
                        help="Path to .txt file – one serial number per line.")
    parser.add_argument("--new-subscription-key", required=True, dest="new_sub_key",
                        help="Subscription key to apply (e.g. SUB-XXXX-XXXX).")
    parser.add_argument("--client-id",     required=True, help="OAuth2 Client ID.")
    parser.add_argument("--client-secret", required=True, help="OAuth2 Client Secret.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate all changes without calling mutating APIs.")
    parser.add_argument("--delay", type=float, default=REQUEST_DELAY,
                        help=f"Seconds between devices (default: {REQUEST_DELAY}).")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = parse_args()

    ts         = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file   = f"glcp_swap_{ts}.log"
    result_csv = f"glcp_swap_results_{ts}.csv"

    logger = setup_logging(log_file)
    logger.info("=== HPE GreenLake Subscription Swap ===")
    if args.dry_run:
        logger.info("*** DRY-RUN mode – no changes will be made ***")

    # Load serials
    try:
        serials = load_serials(args.serials)
    except FileNotFoundError:
        logger.error("Serials file not found: %s", args.serials)
        sys.exit(1)

    if not serials:
        logger.error("No serial numbers found in %s", args.serials)
        sys.exit(1)

    logger.info("Loaded %d serial(s) from %s", len(serials), args.serials)
    logger.info("Target subscription key : %s", args.new_sub_key)

    # Authenticate
    try:
        token, token_expiry = get_access_token(
            args.client_id, args.client_secret, logger)
    except Exception as exc:
        logger.error("Authentication failed: %s", exc)
        sys.exit(1)

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept":        "application/json",
    })

    # Resolve subscription key -> internal UUID once, up front
    if args.dry_run:
        logger.info("[DRY-RUN] Skipping subscription ID resolution.")
        new_sub_id = "DRY-RUN-ID"
    else:
        new_sub_id = get_subscription_id_by_key(session, args.new_sub_key, logger)
        if not new_sub_id:
            logger.error("Cannot continue – subscription key '%s' not found.",
                         args.new_sub_key)
            sys.exit(1)

    # Process each serial
    results       = []
    count_success = 0
    count_fail    = 0

    for idx, serial in enumerate(serials, start=1):
        logger.info("--- [%d/%d] Serial: %s", idx, len(serials), serial)

        # Proactively refresh token if close to expiry
        if time.time() >= token_expiry:
            logger.info("Access token nearing expiry – refreshing ...")
            try:
                token, token_expiry = get_access_token(
                    args.client_id, args.client_secret, logger)
                session.headers.update({"Authorization": f"Bearer {token}"})
            except Exception as exc:
                logger.error("Token refresh failed: %s", exc)
                sys.exit(1)

        record = {
            "serial_number":    serial,
            "device_id":        "",
            "old_subscription": "",
            "new_subscription": args.new_sub_key,
            "status":           "FAILED",
            "error":            "",
        }

        # 1. Look up device
        device = get_device_by_serial(session, serial, logger)
        if device is None:
            record["error"] = "Device not found"
            count_fail += 1
            results.append(record)
            time.sleep(args.delay)
            continue

        record["device_id"] = device.get("id", "")
        current_subs = device.get("subscription") or []
        record["old_subscription"] = ", ".join(
            s.get("key", s.get("id", "?")) for s in current_subs
        )

        # 2. Remove current subscription
        if not remove_subscription(session, device, logger, args.dry_run):
            record["error"] = "Failed to remove subscription"
            count_fail += 1
            results.append(record)
            time.sleep(args.delay)
            continue

        # 3. Re-fetch device to get fresh state before applying
        logger.info("Serial %s – re-fetching device state before apply ...", serial)
        fresh_device = get_device_by_serial(session, serial, logger)
        if fresh_device is None:
            logger.warning("Serial %s – could not re-fetch device; "
                           "using cached record for apply.", serial)
            fresh_device = device

        # 4. Apply new subscription
        if apply_subscription(session, fresh_device, new_sub_id, args.new_sub_key,
                               logger, args.dry_run):
            record["status"] = "DRY-RUN" if args.dry_run else "SUCCESS"
            count_success += 1
        else:
            record["error"] = "Failed to apply new subscription"
            count_fail += 1

        results.append(record)
        time.sleep(args.delay)

    # Write results CSV
    write_results(results, result_csv)

    logger.info("===================================")
    logger.info("Complete. Success: %d  Failed: %d  Total: %d",
                count_success, count_fail, len(serials))
    logger.info("Results : %s", result_csv)
    logger.info("Log     : %s", log_file)

    sys.exit(0 if count_fail == 0 else 1)


if __name__ == "__main__":
    main()
