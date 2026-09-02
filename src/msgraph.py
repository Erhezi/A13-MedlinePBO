import base64
import os
from datetime import datetime, timedelta, timezone

import requests


# Fallbacks used only when a report's config omits the key.
DEFAULT_INBOX_LOOKBACK_DAYS = 6
DEFAULT_ATTACHMENT_PREFIX = ""
DEFAULT_ATTACHMENT_EXTENSIONS = (".xlsx",)


def _format_notification_date(value):
    """Format a configured ISO date for a human-readable email body."""
    try:
        return datetime.fromisoformat(str(value)).strftime("%B %d, %Y").replace(
            " 0", " "
        )
    except (TypeError, ValueError):
        return str(value)


def _raise_for_status_with_details(response, context):
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        detail = ""
        try:
            payload = response.json()
            error_data = payload.get("error")
            if isinstance(error_data, dict):
                error_code = error_data.get("code")
                description = error_data.get("message")
            else:
                error_code = error_data
                description = payload.get("error_description")
            if error_code or description:
                detail = f" ({error_code}: {description})"
        except ValueError:
            pass
        raise requests.HTTPError(f"{context}{detail}", response=response) from exc


def get_access_token(secrets, config):
    """Authenticate via OAuth2 client credentials and return a Bearer token."""
    aad = config["email"]["aad_endpoint"]
    tenant = secrets["TENANT_ID"]
    url = f"{aad}/{tenant}/oauth2/v2.0/token"
    data = {
        "client_id": secrets["CLIENT_ID"],
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": secrets["CLIENT_SECRET"],
        "grant_type": "client_credentials",
    }
    resp = requests.post(url, data=data, timeout=20)
    _raise_for_status_with_details(resp, "Failed to acquire Microsoft Graph access token")
    return resp.json()["access_token"]


def get_folder_id(folder_name, token, config):
    """Return the mail-folder ID for *folder_name*, or None if it is missing."""
    graph = config["email"]["graph_endpoint"]
    user = config["email"]["from_email"]
    headers = {"Authorization": f"Bearer {token}"}

    url = f"{graph}/v1.0/users/{user}/mailFolders"
    params = {"$filter": f"displayName eq '{folder_name}'"}

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    _raise_for_status_with_details(resp, f"Failed to resolve mail folder '{folder_name}'")
    folders = resp.json().get("value", [])

    if folders:
        print(f"Found folder '{folder_name}' with ID: {folders[0]['id']}")
        return folders[0]["id"]

    print(f"Folder '{folder_name}' not found.")
    return None


def _format_graph_datetime(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _list_messages(
    graph,
    user,
    folder_id,
    headers,
    max_messages,
    subject_keyword=None,
    received_since=None,
):
    endpoint = f"{graph}/v1.0/users/{user}/mailFolders/{folder_id}/messages"
    params = {
        "$orderby": "receivedDateTime desc",
        "$select": "id,subject,receivedDateTime",
        "$top": max_messages,
    }

    resp = requests.get(endpoint, headers=headers, params=params, timeout=30)
    _raise_for_status_with_details(resp, f"Failed to list messages in folder '{folder_id}'")
    messages = resp.json().get("value", [])

    if subject_keyword:
        subject_keyword_lower = subject_keyword.lower()
        messages = [
            message
            for message in messages
            if subject_keyword_lower in message.get("subject", "").lower()
        ]

    if received_since is not None:
        messages = [
            message
            for message in messages
            if datetime.fromisoformat(
                message["receivedDateTime"].replace("Z", "+00:00")
            ) >= received_since
        ]

    messages.sort(key=lambda message: message["receivedDateTime"], reverse=True)
    return messages


def _is_target_attachment(
    attachment, attachment_prefix, extensions, contains=(), exclude=()
):
    """Match a Graph attachment against the configured name rules.

    ``contains``/``exclude`` are substring lists that let one mailbox carry
    sibling exports whose names differ only mid-string — e.g. the Allocation
    report's "Product Allocation Report-..." and its companion
    "Product Allocation Report Previous Month-...", which share a subject
    keyword and a name prefix.
    """
    file_name = attachment.get("name", "").lower()
    if attachment.get("@odata.type") != "#microsoft.graph.fileAttachment":
        return False
    if not any(file_name.endswith(ext.lower()) for ext in extensions):
        return False
    if not file_name.startswith(attachment_prefix.lower()):
        return False
    if any(token.lower() not in file_name for token in contains):
        return False
    return not any(token.lower() in file_name for token in exclude)


def _save_attachment(attachment, destination_path):
    file_name = attachment["name"]
    os.makedirs(destination_path, exist_ok=True)
    save_path = os.path.join(destination_path, file_name)
    content_bytes = base64.b64decode(attachment["contentBytes"])

    with open(save_path, "wb") as file_handle:
        file_handle.write(content_bytes)

    print(f"Successfully saved: {save_path}")
    return save_path, file_name


def _find_matching_attachment(
    graph,
    user,
    headers,
    messages,
    destination_path,
    attachment_prefix,
    extensions,
    contains=(),
    exclude=(),
):
    for message in messages:
        print(
            "Checking message "
            f"'{message.get('subject', '')}' ({message['receivedDateTime']})"
        )
        attach_url = f"{graph}/v1.0/users/{user}/messages/{message['id']}/attachments"
        attach_resp = requests.get(attach_url, headers=headers, timeout=30)
        _raise_for_status_with_details(
            attach_resp,
            f"Failed to list attachments for message '{message['id']}'",
        )
        attachments = attach_resp.json().get("value", [])

        for attachment in attachments:
            if _is_target_attachment(
                attachment, attachment_prefix, extensions, contains, exclude
            ):
                print(
                    "Target Found: "
                    f"{message.get('subject', '')} ({message['receivedDateTime']})"
                )
                return _save_attachment(attachment, destination_path)

    return None, None


def get_latest_excel_attachment(
    keyword,
    destination_path,
    config,
    secrets,
    attachment_prefix=None,
    attachment_contains=None,
    attachment_exclude=None,
):
    """Download the newest matching Excel attachment from Graph mail.

    Matching is config-driven (``config['email']``): ``attachment_prefix``,
    ``attachment_contains``, ``attachment_exclude`` and ``attachment_extensions``
    constrain the file name; ``inbox_lookback_days`` (or null for no limit)
    constrains how far back to search.

    The three name filters can be overridden per call so one mailbox can serve
    more than one export — the Allocation report pulls its current-month file
    and its companion "Previous Month" file from the same Inbox and the same
    subject keyword, separated only by ``attachment_contains``/``_exclude``.

    Returns
    -------
    (save_path, file_name) on success.

    Raises
    ------
    FileNotFoundError
        If no matching attachment is found in the Inbox.
    """
    email_cfg = config["email"]
    token = get_access_token(secrets, config)
    headers = {"Authorization": f"Bearer {token}"}
    graph = email_cfg["graph_endpoint"]
    user = email_cfg["from_email"]
    max_messages = email_cfg.get("max_messages", 100)
    if attachment_prefix is None:
        attachment_prefix = email_cfg.get(
            "attachment_prefix", DEFAULT_ATTACHMENT_PREFIX
        )
    if attachment_contains is None:
        attachment_contains = email_cfg.get("attachment_contains", [])
    if attachment_exclude is None:
        attachment_exclude = email_cfg.get("attachment_exclude", [])
    extensions = email_cfg.get(
        "attachment_extensions", list(DEFAULT_ATTACHMENT_EXTENSIONS)
    )
    lookback_days = email_cfg.get("inbox_lookback_days", DEFAULT_INBOX_LOOKBACK_DAYS)

    inbox_cutoff = None
    lookback_note = "all messages"
    if lookback_days is not None:
        inbox_cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        lookback_note = f"the last {lookback_days} days"
    print(f"Searching Inbox for subject containing '{keyword}' from {lookback_note}.")
    inbox_messages = _list_messages(
        graph,
        user,
        folder_id="inbox",
        headers=headers,
        max_messages=max_messages,
        subject_keyword=keyword,
        received_since=inbox_cutoff,
    )
    save_path, file_name = _find_matching_attachment(
        graph,
        user,
        headers,
        inbox_messages,
        destination_path,
        attachment_prefix,
        extensions,
        attachment_contains,
        attachment_exclude,
    )
    if save_path is not None:
        return save_path, file_name

    name_rules = (
        f"starting with '{attachment_prefix}' and ending with one of {extensions}"
    )
    if attachment_contains:
        name_rules += f", containing all of {list(attachment_contains)}"
    if attachment_exclude:
        name_rules += f", containing none of {list(attachment_exclude)}"
    error_message = (
        "Unable to find a matching attachment. "
        f"Inbox search required subject containing '{keyword}' within {lookback_note} "
        f"and attachment name {name_rules}."
    )
    print(f"ERROR: {error_message}")
    raise FileNotFoundError(error_message)


# ── Email sending ────────────────────────────────────────────


def send_email_with_attachment(
    config,
    secrets,
    recipients,
    subject,
    body_text,
    attachment_path=None,
    cc_recipients=None,
):
    """Send an email (with optional attachment) via Microsoft Graph.

    Parameters
    ----------
    recipients : list[str]
        Email addresses to send to.
    attachment_path : str | None
        Path to a local file to attach (or None for no attachment).
    cc_recipients : list[str] | None
        Email addresses to copy on the message.
    """
    token = get_access_token(secrets, config)
    graph = config["email"]["graph_endpoint"]
    sender = config["email"]["from_email"]
    url = f"{graph}/v1.0/users/{sender}/sendMail"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    to_recipients = [
        {"emailAddress": {"address": addr}} for addr in recipients
    ]

    message = {
        "subject": subject,
        "body": {"contentType": "Text", "content": body_text},
        "toRecipients": to_recipients,
    }

    if cc_recipients:
        message["ccRecipients"] = [
            {"emailAddress": {"address": addr}} for addr in cc_recipients
        ]

    if attachment_path and os.path.isfile(attachment_path):
        with open(attachment_path, "rb") as f:
            content = base64.b64encode(f.read()).decode("utf-8")
        message["attachments"] = [
            {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": os.path.basename(attachment_path),
                "contentBytes": content,
            }
        ]

    resp = requests.post(
        url, headers=headers, json={"message": message}, timeout=30,
    )
    _raise_for_status_with_details(resp, "Failed to send email via Graph API")
    print(f"Email sent to {recipients} — {subject}")


def send_success_notification(config, secrets, output_path):
    """Notify success recipients with the output Excel attached.

    Skips silently when no success recipients are configured.
    """
    notification = config["notification"]
    recipients = notification.get("success_recipients", [])
    if not recipients:
        print("No success recipients configured — skipping success notification.")
        return
    report_name = notification.get("report_name", "Report")
    cc_recipients = notification.get("success_cc_recipients", [])
    report_cfg = config.get("report", {})
    desired_dioh_target_date = _format_notification_date(
        report_cfg.get("desired_dioh_target_date", "2026-10-20")
    )
    overstock_check_date = _format_notification_date(
        report_cfg.get("overstock_check_date", "2026-10-13")
    )
    send_email_with_attachment(
        config,
        secrets,
        recipients,
        subject=f"{report_name} Report — Success",
        body_text=(
            f"The {report_name} report has been generated successfully and is attached.\n\n"
            "Current-version calculation notes:\n"
            f"- Desired DIOH is calculated as the number of days between the report "
            f"generation date and {desired_dioh_target_date}.\n"
            f"- ‘Overstocked’ and ‘Not overstocked’ are based on the current burn-rate "
            f"projection against {overstock_check_date}:\n"
            f"  - ‘Not overstocked’ means projected stock is exhausted on or before "
            f"{overstock_check_date}.\n"
            f"  - ‘Overstocked’ means projected stock is expected to last beyond "
            f"{overstock_check_date}."
        ),
        attachment_path=output_path,
        cc_recipients=cc_recipients,
    )


def send_failure_notification(config, secrets, log_path):
    """Notify failure recipients with the error log attached."""
    notification = config["notification"]
    recipients = notification.get("failure_recipients", [])
    if not recipients:
        print("No failure recipients configured — skipping failure notification.")
        return
    report_name = notification.get("report_name", "Report")
    send_email_with_attachment(
        config,
        secrets,
        recipients,
        subject=f"{report_name} Report — Failed",
        body_text=(
            f"The {report_name} report encountered an error. "
            "Please see the attached log for details."
        ),
        attachment_path=log_path,
    )
