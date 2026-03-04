# push_to_github.ps1
# ------------------
# Run this ONCE after installing Git to create the GitHub repo and push all files.
# Prerequisites:
#   1. Install Git from https://git-scm.com/download/win (accept defaults).
#   2. Install GitHub CLI from https://cli.github.com/ (or use web UI to create repo).
#   3. Open a new PowerShell terminal so git.exe is on PATH.
#   4. Authenticate: gh auth login
#
# Then just run:
#   cd C:\Users\Feldberg.Dartmouth\Documents\math86-quanto-project
#   .\push_to_github.ps1

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = "C:\Users\Feldberg.Dartmouth\Documents\math86-quanto-project"
Set-Location $ProjectRoot

Write-Host "=== Initialising local git repo ===" -ForegroundColor Cyan
git init
git add .
git commit -m "Initial commit: Bloomberg blpapi data-pull pipeline"

Write-Host ""
Write-Host "=== Creating GitHub repo 'MATH-86-Quanto-Project' ===" -ForegroundColor Cyan
# --public  : change to --private if preferred
gh repo create "MATH-86-Quanto-Project" --public --description "Bloomberg blpapi FX data pipeline for MATH 86 Quanto Project" --source . --remote origin --push

Write-Host ""
Write-Host "Done!  Visit: https://github.com/$(gh api user --jq '.login')/MATH-86-Quanto-Project" -ForegroundColor Green
