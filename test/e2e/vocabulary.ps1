# E2E CBL-11.3: словарь dim_accounts, backfill и сверка Reporting с Ledger.
# Запуск из каталога сервиса: powershell -NoProfile -File test/e2e/vocabulary.ps1
# Сценарий: compose up (Kafka + ClickHouse + ledger-stub) -> топики -> сервис
# (миграции, конвейер, воркеры словаря и сверки) -> readyz -> поток событий в
# живом времени (окно сверки) -> сид проводок в stub (реальная + фиктивная) ->
# наполнение словаря (счета мерчанта из Ledger, системные деривацией) ->
# backfill истории -> циклы сверки: /metrics подтверждают сверенные счета и
# контролируемое расхождение missing_in_reporting -> down -v.
$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$ComposeFile = Join-Path $RepoRoot "deploy\docker-compose.yml"

# Invoke-Compose выполняет docker compose с подавлением NativeCommandError.
function Invoke-Compose([string[]]$ComposeArgs) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = docker compose -f $ComposeFile @ComposeArgs 2>&1
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    return @{ Output = ($output -join "`n"); Code = $code }
}

# Query-ClickHouse выполняет SQL в контейнере и возвращает TSV-строки.
function Query-ClickHouse([string]$Sql) {
    $result = Invoke-Compose @("exec", "-T", "clickhouse", "clickhouse-client",
        "--user", "cbl", "--password", "cbl", "--format", "TSV", "--query", $Sql)
    if ($result.Code -ne 0) { throw "clickhouse query failed: $Sql`n$($result.Output)" }
    return $result.Output.Trim()
}

# Wait-Query ожидает ожидаемое значение запроса (конвейер асинхронный).
function Wait-Query([string]$Name, [string]$Sql, [string]$Expected) {
    foreach ($attempt in 1..90) {
        $actual = Query-ClickHouse $Sql
        if ($actual -ceq $Expected) { return }
        Start-Sleep -Seconds 2
    }
    throw "$Name`: expected [$Expected], actual [$actual]"
}

# Assert-Query проверяет значение запроса немедленно.
function Assert-Query([string]$Name, [string]$Sql, [string]$Expected) {
    $actual = Query-ClickHouse $Sql
    if ($actual -cne $Expected) { throw "$Name`: expected [$Expected], actual [$actual]" }
}

# Invoke-Producer выполняет сценарий продюсера на хосте и возвращает строки.
function Invoke-Producer([string[]]$ProducerArgs) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = go run ./test/e2e/produce @ProducerArgs 2>&1
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    $text = $output -join "`n"
    if ($code -ne 0) { throw "producer failed: $text" }
    return $text
}

# Field-Value извлекает значение строки вида key=value из вывода продюсера.
function Field-Value([string]$Text, [string]$Key) {
    $line = ($Text -split "`n" | Where-Object { $_ -like "$Key=*" } | Select-Object -First 1)
    if (-not $line) { throw "не найдено $Key в выводе продюсера: $Text" }
    return $line.Substring($Key.Length + 1).Trim()
}

# Invoke-StubEntries сидирует проводки тестового Ledger через HTTP.
function Invoke-StubEntries([string]$AccountId, [array]$Entries) {
    $body = @{ account_id = $AccountId; entries = $Entries } | ConvertTo-Json -Depth 5
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Method Post -TimeoutSec 5 `
            -Uri "http://localhost:9041/entries" -ContentType "application/json" -Body $body
        $code = $response.StatusCode
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($code -ne 204) { throw "stub seed failed: HTTP $code" }
}

# Metric-Value читает счетчик из /metrics сервиса (0, если не найден).
function Metric-Value([string]$MetricName) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $response = Invoke-WebRequest -UseBasicParsing -TimeoutSec 3 http://localhost:9310/metrics
        $text = $response.Content
    } finally {
        $ErrorActionPreference = $previous
    }
    $line = ($text -split "`n" | Where-Object { $_ -like "$MetricName *" } | Select-Object -First 1)
    if (-not $line) { return 0 }
    return [int64]($line -split " ")[-1]
}

# Wait-Metric ожидает счетчик не меньше значения (циклы сверки по интервалу).
function Wait-Metric([string]$Name, [int64]$MinValue) {
    foreach ($attempt in 1..60) {
        $value = Metric-Value $Name
        if ($value -ge $MinValue) { return $value }
        Start-Sleep -Seconds 2
    }
    throw "$Name`: ожидалось не меньше $MinValue, фактически $value"
}

# Assert-Metric проверяет точное значение счетчика.
function Assert-Metric([string]$Name, [int64]$Expected) {
    $value = Metric-Value $Name
    if ($value -ne $Expected) { throw "$Name`: expected $Expected, actual $value" }
}

Push-Location $RepoRoot
try {
    Invoke-Compose @("--profile", "app", "down", "-v") | Out-Null
    # --build пересобирает ledger-stub при изменении исходников (образ без профиля).
    $up = Invoke-Compose @("up", "-d", "--build")
    if ($up.Code -ne 0) { throw "infra up failed: $($up.Output)" }

    # Ожидание готовности Kafka и ClickHouse по healthcheck.
    foreach ($attempt in 1..40) {
        $health = Invoke-Compose @("ps", "--format", "{{.Service}} {{.Health}}")
        if ($health.Output -notmatch "starting") { break }
        Start-Sleep -Seconds 2
    }

    # Топики создаются явно (автосоздание в compose отключено).
    foreach ($topic in @("payment.events", "ledger.events", "payout.events", "recon.alerts")) {
        $create = Invoke-Compose @("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "localhost:9092", "--create", "--topic", $topic,
            "--partitions", "1", "--replication-factor", "1")
        if ($create.Code -ne 0) { throw "topic $topic failed: $($create.Output)" }
    }

    # Сервис применяет миграции, реконсилирует конвейер и запускает воркеры.
    $appUp = Invoke-Compose @("--profile", "app", "up", "-d", "--build")
    if ($appUp.Code -ne 0) { throw "service up failed: $($appUp.Output)" }
    $ready = $false
    foreach ($attempt in 1..60) {
        $previous = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            $response = Invoke-WebRequest -UseBasicParsing -TimeoutSec 3 http://localhost:8110/readyz
            if ($response.StatusCode -eq 200) { $ready = $true; break }
        } catch {
            Start-Sleep -Seconds 2
        } finally {
            $ErrorActionPreference = $previous
        }
    }
    if (-not $ready) { throw "reporting readyz is not green" }

    # Поток событий #1 в живом времени: строки появляются до наполнения словаря.
    $first = Invoke-Producer @("-scenario", "full", "-live")
    $firstPaymentId = Field-Value $first "payment_id"
    $firstTransactionId = Field-Value $first "transaction_id"
    $firstOccurredAt = Field-Value $first "occurred_at"

    # Сид реальной проводки сразу после публикации (до открытия окна сверки):
    # иначе гонка даст ложное missing_in_reporting.
    Invoke-StubEntries "acc-merchant" @(
        @{ transaction_id = $firstTransactionId; direction = "credit"; amount_minor = 100000000; created_at = $firstOccurredAt }
    )

    Wait-Query "payment row" `
        "SELECT count() FROM reporting.fact_payment_events FINAL WHERE payment_id = '$firstPaymentId'" "1"
    Wait-Query "ledger rows" `
        "SELECT count() FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = '$firstTransactionId'" "2"

    # Словарь: счета мерчанта из Ledger (merchant и payout) и три системных
    # счета, производных от пары USDT/6 фактов.
    Wait-Query "dim merchant accounts" `
        "SELECT count() FROM reporting.dim_accounts FINAL WHERE merchant_id = 'm-1'" "2"
    Assert-Query "dim system accounts" `
        "SELECT count() FROM reporting.dim_accounts FINAL WHERE account_type IN ('clearing', 'fee', 'suspense') AND asset = 'USDT' AND decimals = 6" "3"

    # Backfill: историческая строка леджера получила значения словаря
    # (мутации ClickHouse применяются асинхронно).
    Wait-Query "backfill enriched" `
        "SELECT count() FROM reporting.fact_ledger_entries FINAL WHERE account_id = 'acc-merchant' AND account_type = 'merchant' AND merchant_id = 'm-1'" "1"
    # Счет вне плана словаря остается без значений.
    Assert-Query "account outside plan" `
        "SELECT account_type FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = '$firstTransactionId' AND account_id = 'acc-clearing'" ""
    $mutationsBefore = Metric-Value "reporting_backfill_mutations_total"

    # Событие #2 после наполнения словаря: строка леджера обогащается на входе
    # (JOIN MV), backfill-мутаций для нее не требуется.
    $second = Invoke-Producer @("-scenario", "full", "-live")
    $secondPaymentId = Field-Value $second "payment_id"
    $secondTransactionId = Field-Value $second "transaction_id"
    $secondOccurredAt = Field-Value $second "occurred_at"
    $fakeTransactionId = "tx-fake-$([Guid]::NewGuid().ToString())"
    Invoke-StubEntries "acc-merchant" @(
        @{ transaction_id = $firstTransactionId; direction = "credit"; amount_minor = 100000000; created_at = $firstOccurredAt },
        @{ transaction_id = $secondTransactionId; direction = "credit"; amount_minor = 100000000; created_at = $secondOccurredAt },
        @{ transaction_id = $fakeTransactionId; direction = "credit"; amount_minor = 1; created_at = $secondOccurredAt }
    )
    Wait-Query "ledger rows second" `
        "SELECT count() FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = '$secondTransactionId'" "2"
    Wait-Query "enriched at ingest" `
        "SELECT count() FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = '$secondTransactionId' AND account_id = 'acc-merchant' AND account_type = 'merchant' AND merchant_id = 'm-1'" "1"
    Assert-Query "ingest enrichment needs no backfill" `
        "SELECT count() FROM reporting.fact_ledger_entries FINAL WHERE account_id = 'acc-merchant' AND account_type = ''" "0"
    Assert-Metric "reporting_backfill_mutations_total" $mutationsBefore

    # Сверка: циклы идут по интервалу, реальные проводки совпадают, фиктивная
    # дает ровно одно missing_in_reporting; прочих расхождений нет.
    Wait-Metric "reporting_ledger_check_cycles_total" 1 | Out-Null
    Wait-Metric "reporting_ledger_check_accounts_total" 2 | Out-Null
    Wait-Metric 'reporting_ledger_check_discrepancies_total{kind="missing_in_reporting"}' 1 | Out-Null
    Assert-Metric 'reporting_ledger_check_discrepancies_total{kind="extra_in_reporting"}' 0
    Assert-Metric 'reporting_ledger_check_discrepancies_total{kind="amount_mismatch"}' 0
    Assert-Metric "reporting_ledger_check_truncated_total" 0
    Assert-Metric "reporting_ledger_check_errors_total" 0

    Write-Host "OK: vocabulary populated (2 merchant + 3 system accounts), backfill enriched history, ingest enrichment via MV JOIN verified, ledger check cycles green with controlled missing_in_reporting=1"
} finally {
    Invoke-Compose @("--profile", "app", "down", "-v") | Out-Null
    Pop-Location
}
