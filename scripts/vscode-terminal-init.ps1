<#
.SYNOPSIS
    Инициализация терминала VSCode: UTF-8 кодировка + condabin в PATH.
.DESCRIPTION
    Этот скрипт вызывается профилем PowerShell в .vscode/settings.json
    через -NoExit -File, чтобы избежать проблем с парсингом сложных
    inline-команд в командной строкеpwsh.exe.
#>

# Установка кодировки UTF-8 для ввода/вывода
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

# Добавление Anaconda condabin в PATH (если ещё не добавлен)
$condabinPath = "D:\PF\anaconda3\condabin"
if ($env:PATH -notlike "*condabin*") {
    if (Test-Path $condabinPath) {
        $env:PATH = "$condabinPath;" + $env:PATH
    }
}
