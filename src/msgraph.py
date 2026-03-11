import base64
import os

import requests


def _raise_for_status_with_details(response, context):
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        detail = ""
        try:
            payload = response.json()
            error_code = payload.get("error")
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
    """Return the mail-folder ID for *folder_name*, falling back to 'inbox'."""
    graph = config["email"]["graph_endpoint"]
    user = config["email"]["from_email"]
    headers = {"Authorization": f"Bearer {token}"}

    url = f"{graph}/v1.0/users/{user}/mailFolders"
    params = {"$filter": f"displayName eq '{folder_name}'"}

    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    folders = resp.json().get("value", [])

    if folders:
        print(f"Found folder '{folder_name}' with ID: {folders[0]['id']}")
        return folders[0]["id"]

    print(f"Folder '{folder_name}' not found. Falling back to Inbox.")
    return "inbox"


def get_latest_excel_attachment(keyword, destination_path, config, secrets):
    """Download the newest .xlsx attachment whose subject contains *keyword*.

    Returns
    -------
    (save_path, file_name) on success, or (None, None) if nothing found.
    """
    token = get_access_token(secrets, config)
    headers = {"Authorization": f"Bearer {token}"}
    graph = config["email"]["graph_endpoint"]
    user = config["email"]["from_email"]
    folder_name = config["email"]["folder_name"]
    max_messages = config["email"].get("max_messages", 100)

    folder_id = get_folder_id(folder_name, token, config)

    endpoint = f"{graph}/v1.0/users/{user}/mailFolders/{folder_id}/messages"
    params = {
        "$filter": "hasAttachments eq true",
        "$select": "id,subject,receivedDateTime",
        "$top": max_messages,
    }

    resp = requests.get(endpoint, headers=headers, params=params)
    resp.raise_for_status()
    all_messages = resp.json().get("value", [])

    matches = [
        m for m in all_messages
        if keyword.lower() in m.get("subject", "").lower()
    ]
    matches.sort(key=lambda x: x["receivedDateTime"], reverse=True)

    if not matches:
        print(f"No messages found matching '{keyword}'.")
        return None, None

    latest_msg = matches[0]
    print(f"Target Found: {latest_msg['subject']} ({latest_msg['receivedDateTime']})")

    attach_url = f"{graph}/v1.0/users/{user}/messages/{latest_msg['id']}/attachments"
    attach_resp = requests.get(attach_url, headers=headers)
    attach_resp.raise_for_status()
    attachments = attach_resp.json().get("value", [])

    for attachment in attachments:
        file_name = attachment["name"]
        is_xlsx = file_name.lower().endswith(".xlsx")
        is_file = attachment.get("@odata.type") == "#microsoft.graph.fileAttachment"

        if is_xlsx and is_file:
            os.makedirs(destination_path, exist_ok=True)
            save_path = os.path.join(destination_path, file_name)
            content_bytes = base64.b64decode(attachment["contentBytes"])

            with open(save_path, "wb") as f:
                f.write(content_bytes)

            print(f"Successfully saved: {save_path}")
            return save_path, file_name

    print("Email found, but no .xlsx attachment was inside.")
    return None, None
