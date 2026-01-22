param(
    [Parameter(Mandatory=$true)][string]$LogFile,
    [int]$MaxMB = 5,
    [int]$Keep = 5,
    [switch]$RotateDaily,
    [switch]$Compress
)

$maxBytes = $MaxMB * 1MB
$base = $LogFile
$dir = Split-Path $base
$name = [System.IO.Path]::GetFileNameWithoutExtension($base)
$ext  = [System.IO.Path]::GetExtension($base)

if (!(Test-Path $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }

function Rotate-Now {
    # Shift older logs
    for ($i = $Keep; $i -ge 1; $i--) {
        $src = Join-Path $dir ("{0}_{1:00}{2}" -f $name, $i, $ext)
        $dst = Join-Path $dir ("{0}_{1:00}{2}" -f $name, ($i+1), $ext)

        if (Test-Path $src) {
            if ($i -eq $Keep) {
                Remove-Item $src -Force
            } else {
                Rename-Item $src $dst -Force
            }
        }
    }

    # Rotate current base file to _01
    if (Test-Path $base) {
        $rot = Join-Path $dir ("{0}_01{1}" -f $name, $ext)
        Rename-Item $base $rot -Force

        if ($Compress) {
            $zip = $rot + ".zip"
            Compress-Archive -Path $rot -DestinationPath $zip -Force
            Remove-Item $rot -Force
        }
    }
}

# --- Daily rotation check ---
if ($RotateDaily -and (Test-Path $base)) {
    $lastWrite = (Get-Item $base).LastWriteTime.Date
    $today = (Get-Date).Date
    if ($lastWrite -lt $today) {
        Rotate-Now
        exit 0
    }
}

# --- Size rotation check ---
if (Test-Path $base) {
    $len = (Get-Item $base).Length
    if ($len -ge $maxBytes) {
        Rotate-Now
    }
}
