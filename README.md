# Medline PBO Setup and Run Guide

This document is for the person who will run this script on a Windows machine or private server.

The pipeline uses an encrypted application secret. The encrypted values stay in `.env`, and the shared passphrase is stored separately as a Windows environment variable.

The root folder in the examples below is:

```text
your_path_to_this_folder\A13-MedlinePBO
```

## What This Script Does

When `main.py` runs, it:

1. Reads settings from `config.yaml`.
2. Reads secrets from `.env`.
3. Decrypts the Microsoft Graph client secret using the passphrase stored in Windows.
4. Downloads the latest Medline file from email.
5. Processes the report.
6. Saves the output file.
7. Sends a success or failure email.

## Files You Need

Make sure these files are present in the same folder:

1. `main.py`
2. `first_time_setup.py`
3. `config.yaml`
4. `.env`
5. `requirements.txt`
6. The `src` folder and its contents

## 1. First-Time Setup on a New Machine

Do this once per Windows user account.

### Step 1: Open PowerShell in the project folder

Example:

```powershell
cd "your_path_to_this_folder\A13-MedlinePBO"
```

### Step 2: Create and activate the virtual environment if needed

If `.venv` or `venv` already exists, you can skip to Step 3.

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

or:

```powershell
py -3 -m venv venv
.\venv\Scripts\Activate.ps1
```

If PowerShell blocks the activate script, run this once in that PowerShell window:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
```

Then run:

```powershell
.\.venv\Scripts\Activate.ps1
```

or:

```powershell
.\venv\Scripts\Activate.ps1
```

### Step 3: Install Python packages

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Step 4: Run the first-time setup helper

```powershell
python first_time_setup.py
```

What it does:

1. Prompts for the shared passphrase.
2. Saves that passphrase into a persistent Windows user environment variable called `PBO_SECRET_PASSPHRASE`.
3. Offers to convert raw `CLIENT_SECRET` values in `.env` into encrypted `CLIENT_SECRET_HASHED` values.

Important:

1. Use the same passphrase on every machine that will run this pipeline.
2. Store the passphrase somewhere secure outside the code folder.
3. After setup completes, close PowerShell and open a new one before running the pipeline normally.

### Step 5: Test the pipeline

Open a new PowerShell window, go back to the project folder, activate the virtual environment, and run:

```powershell
cd "your_path_to_this_folder\A13-MedlinePBO"
.\.venv\Scripts\Activate.ps1
python main.py
```

or:

```powershell
cd "your_path_to_this_folder\A13-MedlinePBO"
.\venv\Scripts\Activate.ps1
python main.py
```

If setup was correct, the pipeline will start normally.

If you see a message saying the secret passphrase is missing, run:

```powershell
python first_time_setup.py
```

## 2. Subsequent Runs

After the first setup, normal daily use is simple.

### Manual run

Preferred option:

```powershell
cd "your_path_to_this_folder\A13-MedlinePBO"
.\run_medline_pbo.bat
```

This batch file always targets the project's own `.venv\Scripts\python.exe`, `main.py`, and `config.yaml` by absolute path derived from the batch file location, so it does not depend on the caller's working directory.

Direct Python option:

```powershell
cd "your_path_to_this_folder\A13-MedlinePBO"
.\.venv\Scripts\Activate.ps1
python main.py
```

or:

```powershell
cd "your_path_to_this_folder\A13-MedlinePBO"
.\venv\Scripts\Activate.ps1
python main.py
```

You do not need to enter the passphrase again unless the Windows user profile changes or the passphrase needs to be replaced.

### If the client secret changes in the future

If someone updates `.env` with a new raw `CLIENT_SECRET` or `CLIENT_SECRET_FUTURE`, run:

```powershell
python first_time_setup.py
```

or:

```powershell
python encrypt_env.py --hash-secrets-only
```

That will convert the raw values back into encrypted `*_HASHED` values.

## 3. Windows Task Scheduler Setup

Use Task Scheduler if you want Windows to run the pipeline automatically.

Important rule:

The scheduled task must run under the same Windows user account that completed `python first_time_setup.py`, or it must have the same `PBO_SECRET_PASSPHRASE` environment variable configured for the account that runs the task.

### Step 1: Confirm manual run works first

Before scheduling anything, confirm this works in PowerShell:

```powershell
cd "your_path_to_this_folder\A13-MedlinePBO"
.\.venv\Scripts\Activate.ps1
python main.py
```

or:

```powershell
cd "your_path_to_this_folder\A13-MedlinePBO"
.\venv\Scripts\Activate.ps1
python main.py
```

If this manual run fails, do not create the scheduled task yet.

### Step 2: Open Task Scheduler

1. Press Start.
2. Search for `Task Scheduler`.
3. Open it.

### Step 3: Create a new task

1. Click `Create Task...`
2. Give it a name such as `Medline PBO Pipeline`
3. Choose `Run whether user is logged on or not` if needed
4. Choose the correct Windows user account

### Step 4: Add a trigger

Examples:

1. Daily at 11:30 AM
2. Tuesday and Thursday at 11:30 AM (this will be the setting for this PBO Report)
3. After server startup

### Step 5: Add the action

Preferred setup:

Program/script:

```text
your_path_to_this_folder\A13-MedlinePBO\run_medline_pbo.bat
```

Add arguments:

```text
(leave blank)
```

Start in:

```text
(optional) your_path_to_this_folder\A13-MedlinePBO
```

This is the safest option because the batch file builds absolute paths to `.venv\Scripts\python.exe`, `main.py`, and `config.yaml` from its own folder.

Direct Python setup if you do not want to use the batch file:

Program/script:

```text
your_path_to_this_folder\A13-MedlinePBO\.venv\Scripts\python.exe
```

Add arguments:

```text
"your_path_to_this_folder\A13-MedlinePBO\main.py" --config "your_path_to_this_folder\A13-MedlinePBO\config.yaml"
```

Start in:

```text
your_path_to_this_folder\A13-MedlinePBO
```

### Step 6: Save the task and test it

1. Save the task.
2. Right-click the task.
3. Click `Run`.
4. Confirm that the expected output file is created and emails are sent.

### If the scheduled task fails

Check these items first:

1. The task is using the correct Windows user account.
2. That user already ran `python first_time_setup.py`.
3. The Python path in the task points to `.venv\Scripts\python.exe` or `venv\Scripts\python.exe`, whichever exists on that server.
4. The `Start in` folder is the project folder.
5. All required file paths in `config.yaml` are valid on that server.
6. The service account or user account has access to the email account, folders, output location, and database.

## Common Commands

### Run setup

```powershell
python first_time_setup.py
```

### Run the pipeline

```powershell
python main.py
```

### Re-encrypt `.env` secrets if they were updated

```powershell
python encrypt_env.py --hash-secrets-only
```

## Notes for the Manager or Server Owner

1. Do not put the passphrase inside `.env` or `config.yaml`.
2. Keep the passphrase separate from the code.
3. If moving the script to another Windows account, run `python first_time_setup.py` again under that account.
4. If you change the passphrase, you must re-encrypt the secrets in `.env` afterward.
5. The pipeline has a 15-minute timeout. If it runs longer than that, it is terminated and logged as `TIMEOUT`.

## Debugging the Passphrase Setup

If the passphrase appears to be missing even after setup, check it in this order.

### Step 1: Check whether the current PowerShell session can see it

```powershell
$env:PBO_SECRET_PASSPHRASE
```

### Step 2: Check whether Python can see it

```powershell
python -c "import os; print(repr(os.getenv('PBO_SECRET_PASSPHRASE')))"
```

### Step 3: Check whether Windows saved it for the user account

```powershell
[Environment]::GetEnvironmentVariable("PBO_SECRET_PASSPHRASE", "User")
```

How to read the results:

1. If Step 3 shows a value, but Step 1 is blank, the variable was saved correctly but the current terminal did not inherit it.
2. In that case, close VS Code fully or close that PowerShell window, then open a brand new PowerShell window and test again.
3. If Step 3 is blank, run `python first_time_setup.py` again because the value was not persisted.
4. If Steps 1, 2, and 3 all show a value, but `python main.py` still fails, the stored passphrase likely does not match the encrypted values in `.env`.

### Quick temporary test for the current PowerShell window

If you know the correct passphrase, you can set it only for the current shell and test immediately:

```powershell
$env:PBO_SECRET_PASSPHRASE = "your-real-passphrase"
python main.py
```

### Timeout behavior

If the pipeline gets stuck and runs longer than 15 minutes:

1. The worker process is terminated.
2. The existing console output remains in the log file.
3. A timeout message is appended to the end of that log.
4. ETL health is written with status `TIMEOUT`.