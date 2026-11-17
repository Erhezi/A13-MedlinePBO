<#
.SYNOPSIS
    Creates (or replaces) the Medline load tasks in Windows Task Scheduler.

.DESCRIPTION
    Registers one task per report. Each runs this folder's run_medline_<report>.bat
    with --prd (no success emails for these loads; failures email):

      Medline Daily Autosub        Mon-Fri, every 4 hours at :50, 7:50 AM - 3:50 PM
      Medline Order Timing Hourly  every day, hourly at :28, 6:28 AM - 11:28 PM
      Medline Fill-Kill Hourly     every day, hourly at :30, 6:30 AM - 11:30 PM

    PBO and Allocation are not touched.

    All paths come from the folder this script is in, so run it from the
    project folder on the server. A task with the same name is replaced, so
    the script can be run again after a change.

    The tasks run as one Windows account, and that account must be the one
    that ran "python first_time_setup.py" (the passphrase is saved per user).
    By default the script asks for that account's password and sets the tasks
    to run whether the user is logged on or not. The password is needed: the
    runs read the network share and SQL Server as that user. Use -LoggedOnOnly
    to skip the password; the tasks then run only while the user is logged on.

.PARAMETER Report
    Which tasks to set up: autosub, ordertiming, fillkill. Default: all three.

.PARAMETER Mode
    prd (default) or test. test sends every email to the config's test_recipients.

.PARAMETER LoggedOnOnly
    Run only while the account is logged on, without storing a password.

.PARAMETER Credential
    The account to run the tasks as. Default: asks, with the current user filled in.

.PARAMETER Remove
    Delete the selected tasks instead of creating them.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\setup_task_scheduler.ps1
    Sets up all three tasks; asks for the password of the account to run them as.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\setup_task_scheduler.ps1 -Report ordertiming,fillkill

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\setup_task_scheduler.ps1 -WhatIf
    Shows what would be set up, without changing anything.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\setup_task_scheduler.ps1 -Report autosub -Remove
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [ValidateSet('autosub', 'ordertiming', 'fillkill')]
    [string[]] $Report = @('autosub', 'ordertiming', 'fillkill'),

    [ValidateSet('prd', 'test')]
    [string] $Mode = 'prd',

    [switch] $LoggedOnOnly,

    [System.Management.Automation.PSCredential] $Credential,

    [switch] $Remove
)

$ErrorActionPreference = 'Stop'
$ProjectDir = $PSScriptRoot

# ---- Task definitions ------------------------------------------------------
# Start is local time. The trigger carries no UTC offset, so the runs keep
# their clock time across daylight-saving changes.
# Duration is how long the hourly repeat lasts after Start; it ends a little
# after the last run (6:30 AM + 17h29m = 11:59 PM, last run 11:30 PM).
$Tasks = [ordered]@{
    autosub     = @{
        Name        = 'Medline Daily Autosub'
        Bat         = 'run_medline_autosub.bat'
        Start       = '07:50'
        Interval    = 'PT4H'
        Duration    = 'PT9H10M'
        WeekdaysOnly = $true
        Description = 'Medline Daily Autosub Management load (A13-MedlinePBO, --report medline_autosub). Every 4 hours Mon-Fri, 7:50 AM until 5:00 PM (last run 3:50 PM); loads each new Medline export once, otherwise skips.'
    }
    ordertiming = @{
        Name        = 'Medline Order Timing Hourly'
        Bat         = 'run_medline_ordertiming.bat'
        Start       = '06:28'
        Interval    = 'PT1H'
        Duration    = 'PT17H31M'
        WeekdaysOnly = $false
        Description = 'Medline Order Timing hourly load (A13-MedlinePBO, --report medline_ordertiming). Every day, hourly at :28 from 6:28 AM until 11:59 PM (last run 11:28 PM), 2 min before Fill-Kill; replaces the newest MHS - ORDER DETAIL REPORT mail''s Report Date in MedlineOrderTiming.MedlineOrderTiming.'
    }
    fillkill    = @{
        Name        = 'Medline Fill-Kill Hourly'
        Bat         = 'run_medline_fillkill.bat'
        Start       = '06:30'
        Interval    = 'PT1H'
        Duration    = 'PT17H29M'
        WeekdaysOnly = $false
        Description = 'Medline Fill-Kill hourly load (A13-MedlinePBO, --report medline_fillkill). Every day, hourly at :30 from 6:30 AM until 11:59 PM (last run 11:30 PM); upserts the newest MHS - FILL KILL REPORT mail into MedlineFillKill.MedlineFillKill.'
    }
}

function Escape-Xml([string] $text) {
    return [System.Security.SecurityElement]::Escape($text)
}

function New-TaskXml($task, [string] $userId, [string] $logonType) {
    $bat = Join-Path $ProjectDir $task.Bat
    $startBoundary = (Get-Date -Format 'yyyy-MM-dd') + 'T' + $task.Start + ':00'
    if ($task.WeekdaysOnly) {
        $schedule = @'
      <ScheduleByWeek>
        <WeeksInterval>1</WeeksInterval>
        <DaysOfWeek><Monday /><Tuesday /><Wednesday /><Thursday /><Friday /></DaysOfWeek>
      </ScheduleByWeek>
'@
    } else {
        $schedule = @'
      <ScheduleByDay>
        <DaysInterval>1</DaysInterval>
      </ScheduleByDay>
'@
    }
    return @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.3" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>$(Escape-Xml $task.Description)</Description>
  </RegistrationInfo>
  <Principals>
    <Principal id="Author">
      <UserId>$(Escape-Xml $userId)</UserId>
      <LogonType>$logonType</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <ExecutionTimeLimit>PT30M</ExecutionTimeLimit>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <StartWhenAvailable>true</StartWhenAvailable>
    <Enabled>true</Enabled>
  </Settings>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>$startBoundary</StartBoundary>
      <ExecutionTimeLimit>PT30M</ExecutionTimeLimit>
      <Repetition>
        <Interval>$($task.Interval)</Interval>
        <Duration>$($task.Duration)</Duration>
      </Repetition>
$schedule    </CalendarTrigger>
  </Triggers>
  <Actions Context="Author">
    <Exec>
      <Command>"$(Escape-Xml $bat)"</Command>
      <Arguments>--$Mode</Arguments>
      <WorkingDirectory>$(Escape-Xml $ProjectDir)</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"@
}

$selected = @($Tasks.Keys | Where-Object { $Report -contains $_ })

# ---- Remove ----------------------------------------------------------------
if ($Remove) {
    foreach ($key in $selected) {
        $name = $Tasks[$key].Name
        if (-not (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue)) {
            Write-Host "Not found, nothing to remove: $name"
        } elseif ($PSCmdlet.ShouldProcess($name, 'Unregister scheduled task')) {
            Unregister-ScheduledTask -TaskName $name -Confirm:$false
            Write-Host "Removed: $name"
        }
    }
    return
}

# ---- Checks before anything is registered ----------------------------------
Write-Host "Project folder: $ProjectDir"
foreach ($key in $selected) {
    $bat = Join-Path $ProjectDir $Tasks[$key].Bat
    if (-not (Test-Path -LiteralPath $bat)) { throw "Launcher not found: $bat" }
}
$python = @('venv', '.venv') |
    ForEach-Object { Join-Path $ProjectDir "$_\Scripts\python.exe" } |
    Where-Object { Test-Path -LiteralPath $_ } |
    Select-Object -First 1
if ($python) {
    Write-Host "Python: $python"
} else {
    Write-Warning "No venv\Scripts\python.exe or .venv\Scripts\python.exe under $ProjectDir; the tasks will fail until one is created (see README, section 1)."
}

$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
if ($LoggedOnOnly) {
    $runAs = $currentUser
    $logonType = 'InteractiveToken'
    $password = $null
} else {
    if (-not $Credential) {
        $Credential = Get-Credential -UserName $currentUser `
            -Message 'Account the Medline tasks run as (the one that ran first_time_setup.py)'
        if (-not $Credential) { throw 'No credential given; nothing was changed.' }
    }
    $runAs = $Credential.UserName
    $logonType = 'Password'
    $password = $Credential.GetNetworkCredential().Password
}

if ($runAs -ieq $currentUser -and
    -not [Environment]::GetEnvironmentVariable('PBO_SECRET_PASSPHRASE', 'User')) {
    Write-Warning "$runAs has no PBO_SECRET_PASSPHRASE user variable; run 'python first_time_setup.py' as this user, or every run will fail."
}

# ---- Register --------------------------------------------------------------
foreach ($key in $selected) {
    $task = $Tasks[$key]
    $xml = New-TaskXml $task $runAs $logonType
    $action = "Register as $runAs ($logonType), --$Mode"
    if (-not $PSCmdlet.ShouldProcess($task.Name, $action)) { continue }

    $register = @{ TaskName = $task.Name; Xml = $xml; Force = $true }
    if ($logonType -eq 'Password') {
        $register.User = $runAs
        $register.Password = $password
    }
    Register-ScheduledTask @register | Out-Null
    Write-Host "Registered: $($task.Name)"
}

if ($WhatIfPreference) { return }

# ---- Summary ---------------------------------------------------------------
Write-Host ''
foreach ($key in $selected) {
    $name = $Tasks[$key].Name
    $info = Get-ScheduledTaskInfo -TaskName $name
    $t = Get-ScheduledTask -TaskName $name
    Write-Host ("{0,-28} next run {1:yyyy-MM-dd HH:mm}   {2} {3}" -f `
        $name, $info.NextRunTime, $t.Actions[0].Execute, $t.Actions[0].Arguments)
}
Write-Host ''
Write-Host 'To try one now:  Start-ScheduledTask -TaskName "Medline Order Timing Hourly"'
Write-Host 'Then check its log folder (logging.log_dir in the report''s config.yaml) and the ETL-health table.'
