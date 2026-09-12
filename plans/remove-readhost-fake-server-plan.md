# План: убрать автозавершение из run_fake_server.ps1 и FakeTickServer

## Цель

1. Убрать `Read-Host` из `run_fake_server.ps1` — автозавершение программы.
2. Убрать самозавершение FakeTickServer при достижении MaxTicks (`StopApplication()`).
3. Перенести логику завершения в `start_loadtest.ps1` — оркестратор сам убивает FakeTickServer после graceful остановки Worker.

## Мотивация

- `start_loadtest.ps1` — единый оркестратор, он должен контролировать жизненный цикл всех процессов.
- FakeTickServer не должен решать, когда ему завершаться — это ответственность оркестратора.
- При самозавершении FakeTickServer Worker может получить WebSocket-ошибки вместо NormalClosure.

## Изменения

### 1. `run_fake_server.ps1` — удалить `Read-Host`

**Файл:** [`run_fake_server.ps1`](../run_fake_server.ps1:47)

**Что сделать:** удалить строку 47:
```powershell
Read-Host -Prompt "Нажмите любую клавишу для выхода"
```

**Результат:** скрипт запускает сервер через `dotnet run` и просто завершается. Сервер остаётся работать в фоне.

---

### 2. `tests/FakeTickServer/TickGeneratorService.cs` — убрать `StopApplication()`

**Файл:** [`tests/FakeTickServer/TickGeneratorService.cs`](../tests/FakeTickServer/TickGeneratorService.cs)

Есть два места, где вызывается `_hostLifetime.StopApplication()`:
- строка 225 — в начале цикла при проверке MaxTicks
- строка 341 — после внутреннего цикла отправки при достижении MaxTicks

**Что сделать:** заменить оба вызова `_hostLifetime.StopApplication()` на простой `return` (метод и так завершается).

Было (строка 225):
```csharp
                    _hostLifetime.StopApplication();
                    return;
```
Стало:
```csharp
                    return;
```

Было (строка 341):
```csharp
                            _hostLifetime.StopApplication();
                            return;
```
Стало:
```csharp
                            return;
```

**Результат:** FakeTickServer перестаёт генерировать тики при достижении MaxTicks, но процесс остаётся жить. WebSocket-клиенты остаются подключёнными.

---

### 3. `start_loadtest.ps1` — обновить логику завершения FakeTickServer

**Файл:** [`start_loadtest.ps1`](../start_loadtest.ps1)

Сейчас раздел `[6/7]` ждёт, пока FakeTickServer сам завершится. После изменений сервер не завершится сам — его нужно убивать принудительно после остановки Worker.

**Что сделать:** изменить разделы:

#### 3a. Раздел `[6/7] Ожидание завершения FakeTickServer` (строки 271–286)

Заменить на ожидание того, что MaxTicks достигнут (проверка через health-эндпоинт или просто таймаут), затем принудительная остановка:

```powershell
Write-Step "[6/7] Ожидание завершения генерации FakeTickServer"
# FakeServer не завершается сам — ждём расчётное время генерации MaxTicks,
# затем убиваем процесс.
$generationSeconds = [Math]::Max(30, [int]($MaxTicks / [Math]::Max(1, $Rps)) + 30)
Write-Host "  Ожидание генерации $MaxTicks тиков (~${generationSeconds}с)..."
Start-Sleep -Seconds $generationSeconds
Write-Host "  Генерация завершена. Пауза для дренажа очередей Worker (15с)..." -ForegroundColor Yellow
Start-Sleep -Seconds 15
```

#### 3b. Раздел `[7/7] Graceful остановка Worker` (строки 291–309)

После остановки Worker добавить принудительное завершение FakeTickServer:

```powershell
# после Worker shutdown...
# Принудительная остановка FakeTickServer
Write-Host "  Остановка FakeTickServer..." -ForegroundColor Yellow
taskkill /F /IM FakeTickServer.exe 2>$null
Start-Sleep -Seconds 1
```

---

### 4. `start_loadtest.ps1` — добавить `Read-Host` в конец

Как и в предыдущем плане — после итоговой сводки добавить:
```powershell
Read-Host -Prompt "Нажмите любую клавишу для выхода"
```

## Проверка

1. `run_fake_server.ps1` — запускает сервер, не ждёт нажатия клавиши.
2. `start_loadtest.ps1`:
   - Запускает FakeTickServer (он генерирует MaxTicks, но не завершается).
   - Запускает Worker + Profiler.
   - Ждёт расчётное время генерации.
   - Graceful stop Worker через `/shutdown`.
   - Принудительно убивает FakeTickServer.
   - Показывает итоговую сводку и ждёт `Read-Host`.
3. FakeTickServer не завершается сам по достижении MaxTicks — только по сигналу оркестратора.