import base64
import os
from secrets import token_bytes

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt


SECRET_PREFIX = "enc::"
SECRET_ENV_VAR = "PBO_SECRET_PASSPHRASE"
SALT_LEN = 16
NONCE_LEN = 12


def _derive_key(passphrase: str, salt: bytes) -> bytes:
    kdf = Scrypt(salt=salt, length=32, n=2**14, r=8, p=1)
    return kdf.derive(passphrase.encode("utf-8"))


def get_secret_passphrase(required=True):
    passphrase = os.getenv(SECRET_ENV_VAR, "").strip()
    if required and not passphrase:
        raise RuntimeError(
            f"Set the {SECRET_ENV_VAR} environment variable before loading encrypted secrets."
        )
    return passphrase


def encrypt_secret_value(value: str, passphrase: str | None = None) -> str:
    if passphrase is None:
        passphrase = get_secret_passphrase(required=True)

    salt = token_bytes(SALT_LEN)
    nonce = token_bytes(NONCE_LEN)
    key = _derive_key(passphrase, salt)
    ciphertext = AESGCM(key).encrypt(nonce, value.encode("utf-8"), None)
    payload = base64.urlsafe_b64encode(salt + nonce + ciphertext).decode("ascii")
    return f"{SECRET_PREFIX}{payload}"


def decrypt_secret_value(value: str, passphrase: str | None = None) -> str:
    if not value or not value.startswith(SECRET_PREFIX):
        return value

    if passphrase is None:
        passphrase = get_secret_passphrase(required=True)

    payload = base64.urlsafe_b64decode(value[len(SECRET_PREFIX):].encode("ascii"))
    salt = payload[:SALT_LEN]
    nonce = payload[SALT_LEN:SALT_LEN + NONCE_LEN]
    ciphertext = payload[SALT_LEN + NONCE_LEN:]
    key = _derive_key(passphrase, salt)
    plaintext = AESGCM(key).decrypt(nonce, ciphertext, None)
    return plaintext.decode("utf-8")


def encrypt_secret_env_lines(lines, secret_keys, passphrase: str | None = None):
    updated_lines = []
    updated_keys = []

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            updated_lines.append(line)
            continue

        line_ending = "\n" if line.endswith("\n") else ""
        content = line[:-1] if line_ending else line
        key, _, raw_value = content.partition("=")
        normalized_key = key.strip()

        if normalized_key not in secret_keys:
            updated_lines.append(line)
            continue

        value_text = raw_value.rstrip()
        comment = ""
        comment_index = value_text.find(" #")
        if comment_index != -1:
            comment = value_text[comment_index:]
            value_text = value_text[:comment_index]

        secret_value = value_text.strip().strip('"').strip("'")
        hashed_key = f"{normalized_key}_HASHED"
        hashed_value = encrypt_secret_value(secret_value, passphrase=passphrase)
        updated_lines.append(f"{hashed_key}={hashed_value}{comment}{line_ending}")
        updated_keys.append(normalized_key)

    return updated_lines, updated_keys