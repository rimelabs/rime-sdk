# Run after the production wheel and npm tarball have been built and installed.
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$root = Split-Path $PSScriptRoot -Parent
$clean = Join-Path ([IO.Path]::GetTempPath()) ("rime-windows-clean-" + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $clean | Out-Null

# Copy only the Python and Node distributions and the installed release packages.
# In particular, do not copy Visual Studio or any DLL from the runner's System32.
$python = (& uv python find 3.11).Trim()
if ($LASTEXITCODE -ne 0) { throw 'Could not locate Python' }
Copy-Item (Split-Path $python -Parent) "$clean/python" -Recurse
Copy-Item (Get-Command node).Source "$clean/node.exe"
$wheels = @(Get-ChildItem "$root/artifacts/wheels/*.whl")
if ($wheels.Count -ne 1) { throw 'Expected one Windows wheel' }
& uv pip install --python $python --no-deps --target "$clean/site-packages" $wheels[0].FullName
if ($LASTEXITCODE -ne 0) { throw 'Could not install the release wheel' }
Copy-Item "$root/artifacts/node-install/node_modules" "$clean/node_modules" -Recurse
Copy-Item "$PSScriptRoot/smoke-python.py", "$PSScriptRoot/smoke-node.mjs", "$PSScriptRoot/smoke-windows.ps1" $clean

# Check both installed packages, including delay-load dependencies. The host
# interpreters can carry their own CRT, so a successful import alone is not enough.
$vswhere = "${env:ProgramFiles(x86)}/Microsoft Visual Studio/Installer/vswhere.exe"
$dumpbin = @(& $vswhere -latest -products '*' -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -find 'VC/Tools/MSVC/**/bin/Hostx64/x64/dumpbin.exe')
if ($LASTEXITCODE -ne 0 -or $dumpbin.Count -eq 0) { throw 'Could not locate dumpbin' }
$binaries = @(Get-ChildItem "$clean/site-packages", "$clean/node_modules" -Recurse -File |
    Where-Object { $_.Extension -in '.pyd', '.node', '.dll' })
if (@($binaries | Where-Object Extension -eq '.pyd').Count -eq 0 -or
    @($binaries | Where-Object Extension -eq '.node').Count -eq 0) {
    throw 'Missing installed native packages'
}
foreach ($binary in $binaries) {
    $imports = & $dumpbin[0] /DEPENDENTS $binary.FullName
    if ($LASTEXITCODE -ne 0) { throw "Could not inspect $($binary.FullName)" }
    if ($imports -match '(?i)\b(?:msvcp|msvcr|vcruntime|concrt|vcomp)\d[^\s]*\.dll\b') {
        throw "Native package requires the Visual C++ runtime: $($binary.FullName)`n$imports"
    }
}

# The pinned runner and container must use the same Windows kernel generation.
& docker run --rm --isolation=process --network=none `
    --mount "type=bind,source=$clean,target=C:\test,readonly" `
    mcr.microsoft.com/windows/servercore:ltsc2022 `
    powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File C:\test\smoke-windows.ps1
if ($LASTEXITCODE -ne 0) { throw 'Clean Windows package test failed' }
Remove-Item -LiteralPath $clean -Recurse -Force
