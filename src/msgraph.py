import base64
import os
from datetime import datetime, timedelta, timezone

import requests


INBOX_LOOKBACK_HOURS = 48
TARGET_ATTACHMENT_PREFIX = "Proactive"


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


def _is_target_attachment(attachment, attachment_prefix):
    file_name = attachment.get("name", "")
    return (
        attachment.get("@odata.type") == "#microsoft.graph.fileAttachment"
        and file_name.lower().endswith(".xlsx")
        and file_name.lower().startswith(attachment_prefix.lower())
    )


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
            if _is_target_attachment(attachment, attachment_prefix):
                print(
                    "Target Found: "
                    f"{message.get('subject', '')} ({message['receivedDateTime']})"
                )
                return _save_attachment(attachment, destination_path)

    return None, None


def get_latest_excel_attachment(keyword, destination_path, config, secrets):
    """Download the newest Proactive .xlsx attachment from Graph mail.

    Returns
    -------
    (save_path, file_name) on success.

    Raises
    ------
    FileNotFoundError
        If no matching attachment is found in either Inbox or the fallback folder.
    """
    token = get_access_token(secrets, config)
    headers = {"Authorization": f"Bearer {token}"}
    graph = config["email"]["graph_endpoint"]
    user = config["email"]["from_email"]
    folder_name = config["email"]["folder_name"]
    max_messages = config["email"].get("max_messages", 100)

    inbox_cutoff = datetime.now(timezone.utc) - timedelta(hours=INBOX_LOOKBACK_HOURS)
    print(
        f"Searching Inbox for subject containing '{keyword}' from the last "
        f"{INBOX_LOOKBACK_HOURS} hours."
    )
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
        TARGET_ATTACHMENT_PREFIX,
    )
    if save_path is not None:
        return save_path, file_name

    print(
        f"No '{TARGET_ATTACHMENT_PREFIX}*.xlsx' attachment found in Inbox. "
        f"Searching folder '{folder_name}' next."
    )
    folder_id = get_folder_id(folder_name, token, config)
    if folder_id is not None:
        folder_messages = _list_messages(
            graph,
            user,
            folder_id=folder_id,
            headers=headers,
            max_messages=max_messages,
        )
        save_path, file_name = _find_matching_attachment(
            graph,
            user,
            headers,
            folder_messages,
            destination_path,
            TARGET_ATTACHMENT_PREFIX,
        )
        if save_path is not None:
            return save_path, file_name

    error_message = (
        "Unable to find a matching attachment. "
        f"Inbox search required subject containing '{keyword}' within the last "
        f"{INBOX_LOOKBACK_HOURS} hours and attachment name starting with "
        f"'{TARGET_ATTACHMENT_PREFIX}'. Fallback folder '{folder_name}' also "
        "did not contain a matching attachment in the latest messages."
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
    """Notify success recipients with the output Excel attached."""
    recipients = config["notification"]["success_recipients"]
    cc_recipients = config["notification"].get("success_cc_recipients", [])
    send_email_with_attachment(
        config,
        secrets,
        recipients,
        subject="Medline PBO Report — Success",
        body_text="The Medline PBO report is analyzed and enriched.",
        attachment_path=output_path,
        cc_recipients=cc_recipients,
    )


def send_failure_notification(config, secrets, log_path):
    """Notify failure recipients with the error log attached."""
    recipients = config["notification"]["failure_recipients"]
    send_email_with_attachment(
        config,
        secrets,
        recipients,
        subject="Medline PBO Report — Failed",
        body_text=(
            "The Medline PBO report encountered an error. "
            "Please see the attached log for details."
        ),
        attachment_path=log_path,
    )
