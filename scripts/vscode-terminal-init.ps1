<#
.SYNOPSIS
    Инициализация терминала VSCode: UTF-8 кодировка + condabin в PATH.
.DESCRIPTION
    Этот скрипт вызывается профилем PowerShell в .vscode/settings.json
    через -NoExit -File, чтобы избежать проблем с парсингом сложных
    inline-команд в командной строкеpwsh.exe.

    Дополнительно настраивает UTF-8 кодировку для ввода/вывода и для
    дочерних процессов, включая cmd.exe, чтобы кириллица в выводе
    команд не превращалась в "кракозябры".
#>

# --- Кодировка консоли (ввод/вывод самого PowerShell) ---
$utf8 = [System.Text.UTF8Encoding]::new($false) # UTF-8 без BOM
[Console]::InputEncoding  = $utf8
[Console]::OutputEncoding = $utf8

# --- Кодировка для внешних команд и pipeline ---
$OutputEncoding = $utf8
$InputEncoding  = $utf8

# --- Переменные окружения для внешних процессов ---
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUTF8       = "1"

# --- Принудительное переключение кодовой страницы консоли на UTF-8 ---
try {
    & cmd.exe /c "chcp 65001 >nul"
} catch {
    # консоль может быть недоступна (неинтерактивный запуск) — игнорируем
}

# --- Примечание ---
# Встроенную команду cmd мы намеренно НЕ переопределяем, чтобы не ломать
# существующие вызовы "cmd /c ...". Для корректного вывода кириллицы
# достаточно переключить кодовую страницу консоли на UTF-8 (chcp 65001),
# выполненного выше, и установить кодировки ввода/вывода.

# Добавление Anaconda condabin в PATH (если ещё не добавлен)
$condabinPath = "D:\PF\anaconda3\condabin"
if ($env:PATH -notlike "*condabin*") {
    if (Test-Path $condabinPath) {
        $env:PATH = "$condabinPath;" + $env:PATH
    }
}
