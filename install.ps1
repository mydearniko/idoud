param(
  [string]$Repo = "mydearniko/idoud",
  [string]$BinaryName = "idoud",
  [string]$InstallDir = "$env:USERPROFILE\bin"
)

$ErrorActionPreference = "Stop"

function Get-Arch {
  $arch = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
  switch ($arch.ToLowerInvariant()) {
    "amd64" { return "amd64" }
    "x86" { return "i386" }
    "arm64" { return "arm64" }
    default { throw "unsupported Windows architecture: $arch" }
  }
}

$arch = Get-Arch
$assets = @("${BinaryName}_windows_${arch}.exe")
if ($arch -eq "amd64") {
  $assets += "${BinaryName}_windows_amd64_v2.exe"
  $assets += "${BinaryName}_windows_amd64_v1.exe"
}

New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null

$tempFile = Join-Path ([System.IO.Path]::GetTempPath()) ("$BinaryName-" + [Guid]::NewGuid().ToString("N") + ".exe")
$targetFile = Join-Path $InstallDir "$BinaryName.exe"

$downloadedAsset = $null
foreach ($asset in $assets) {
  $url = "https://github.com/$Repo/releases/latest/download/$asset"
  try {
    Invoke-WebRequest -Uri $url -OutFile $tempFile
    $downloadedAsset = $asset
    break
  } catch {
    Remove-Item -Force -ErrorAction SilentlyContinue $tempFile
  }
}

if ($null -eq $downloadedAsset) {
  throw "no matching release asset found for Windows/$arch"
}

Write-Host "Downloaded $downloadedAsset from $Repo."
Move-Item -Path $tempFile -Destination $targetFile -Force

$currentPath = [Environment]::GetEnvironmentVariable("Path", "User")
$normalizedInstall = $InstallDir.TrimEnd("\")
$pathEntries = @()
if (-not [string]::IsNullOrWhiteSpace($currentPath)) {
  $pathEntries = $currentPath.Split(";") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
}

if (-not ($pathEntries | ForEach-Object { $_.TrimEnd("\") } | Where-Object { $_ -ieq $normalizedInstall })) {
  $newPath = if ([string]::IsNullOrWhiteSpace($currentPath)) { $InstallDir } else { "$currentPath;$InstallDir" }
  [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
  $env:Path = "$env:Path;$InstallDir"
  Write-Host "Added $InstallDir to your user PATH (open a new terminal if needed)."
}

Write-Host "Installed: $targetFile"
Write-Host "Run: $BinaryName --help"
