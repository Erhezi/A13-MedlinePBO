import os
import yaml

from src.secret_crypto import decrypt_secret_value


def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _normalize_secret_value(value):
    value = value.strip()
    if not value:
        return value

    if value[0] in {'"', "'"}:
        quote = value[0]
        if value.endswith(quote):
            return value[1:-1]
        return value

    comment_index = value.find(" #")
    if comment_index != -1:
        value = value[:comment_index]

    return value.strip()


def load_secrets(env_path=".env"):
    secrets = {}
    if not os.path.exists(env_path):
        raise FileNotFoundError(
            f"{env_path} not found. Run 'python decrypt_env.py' first to decrypt .env.enc"
        )
    with open(env_path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            key, _, value = line.partition("=")
            if not key:
                continue
            secrets[key.strip()] = _normalize_secret_value(value)

    for secret_key in ("CLIENT_SECRET", "CLIENT_SECRET_FUTURE"):
        hashed_key = f"{secret_key}_HASHED"
        hashed_value = secrets.get(hashed_key)
        if hashed_value:
            secrets[secret_key] = decrypt_secret_value(hashed_value)
    return secrets
