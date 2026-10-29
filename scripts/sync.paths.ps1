# Dot-sourced by every script in this folder.
# $ProfileName is the ~/.databrickscfg profile; $RemoteBase is your user folder in the workspace.
$ProfileName = "adb-7405614283227928"
$ProjectRoot = "databricks-project"
$RemoteBase  = "/Workspace/Users/darko.martinovic_outlook.com#ext#@darkomartinovicoutlook.onmicrosoft.com"

# Every notebook under databricks-project/src syncs to the same relative path under $RemoteBase,
# without the .ipynb extension: src/Streaming/01 - X.ipynb -> $RemoteBase/Streaming/01 - X
$srcRoot = Join-Path (Split-Path -Parent $PSScriptRoot) "$ProjectRoot/src"
$NotebookMappings = @(
	Get-ChildItem -Path $srcRoot -Filter *.ipynb -Recurse |
		Sort-Object FullName |
		ForEach-Object {
			$relative = $_.FullName.Substring($srcRoot.Length + 1).Replace('\', '/')
			@{
				Local  = "$ProjectRoot/src/$relative"
				Remote = "$RemoteBase/" + $relative.Substring(0, $relative.Length - ".ipynb".Length)
				Format = "JUPYTER"
			}
		}
)

# spark_setup.py syncs as a Python notebook, so it must keep its "# Databricks notebook source" header
$NotebookMappings += @{
	Local    = "$ProjectRoot/src/spark_setup.py"
	Remote   = "$RemoteBase/spark_setup"
	Format   = "SOURCE"
	Language = "PYTHON"
}
