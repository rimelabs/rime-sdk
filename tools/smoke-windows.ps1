# This script runs inside a fresh Server Core container, without the VC redist.
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$runtime = @(Get-ChildItem "$env:WINDIR/System32", "$env:WINDIR/SysWOW64" -File |
    Where-Object { $_.Name -match '^(msvcp|vcruntime|concrt|vcomp)140.*\.dll$' })
if ($runtime.Count) { throw 'The clean Windows image contains the VC runtime' }

Set-Location C:\test
$env:PATH = "C:\test\python;C:\test;$env:WINDIR\System32;$env:WINDIR"
$env:PYTHONPATH = 'C:\test\site-packages'
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:PYTHONNOUSERSITE = '1'
& C:\test\python\python.exe C:\test\smoke-python.py
if ($LASTEXITCODE -ne 0) { throw 'Python package failed on clean Windows' }
& C:\test\node.exe C:\test\smoke-node.mjs
if ($LASTEXITCODE -ne 0) { throw 'Node package failed on clean Windows' }
