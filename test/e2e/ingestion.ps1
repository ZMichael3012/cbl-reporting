# E2E CBL-11.2: конвейер ClickHouse на compose-стенде сервиса.
# Запуск из каталога сервиса: powershell -NoProfile -File test/e2e/ingestion.ps1
# Сценарий: compose up (Kafka + ClickHouse) -> топики -> старт сервиса
# (миграции fact_*, реконсиляция Kafka engine + MV) -> readyz -> продюсер
# full (4 события) -> строки в fact_* со значениями -> duplicate (дедуп FINAL)
# -> broken (конвейер продолжает работу) -> down -v.
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
    foreach ($attempt in 1..60) {
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

Push-Location $RepoRoot
try {
    Invoke-Compose @("--profile", "app", "down", "-v") | Out-Null
    $up = Invoke-Compose @("up", "-d")
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

    # Сервис применяет миграции и реконсилирует конвейер при старте.
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

    # Конвейер сконфигурирован: 4 Kafka-таблицы и 4 materialized view.
    Assert-Query "kafka tables" `
        "SELECT count() FROM system.tables WHERE database = 'reporting' AND name LIKE 'kafka_%'" "4"
    Assert-Query "views" `
        "SELECT count() FROM system.tables WHERE database = 'reporting' AND name LIKE '%_mv'" "4"

    # Полный поток событий.
    $full = Invoke-Producer @("-scenario", "full")
    $paymentId = Field-Value $full "payment_id"
    $paymentEventId = Field-Value $full "payment_event_id"
    $transactionId = Field-Value $full "transaction_id"
    $payoutId = Field-Value $full "payout_id"
    $alertId = Field-Value $full "alert_id"

    Wait-Query "payment row" `
        "SELECT count() FROM reporting.fact_payment_events FINAL WHERE payment_id = '$paymentId'" "1"
    Wait-Query "ledger rows" `
        "SELECT count() FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = '$transactionId'" "2"
    Wait-Query "payout row" `
        "SELECT count() FROM reporting.fact_payouts FINAL WHERE payout_id = '$payoutId'" "1"
    Wait-Query "alert row" `
        "SELECT count() FROM reporting.fact_recon_alerts FINAL WHERE alert_id = '$alertId'" "1"

    # Значения: деньги, тип из заголовка, entry_index и направление.
    Assert-Query "payment values" `
        "SELECT concat(event_type, '|', toString(expected_amount), '|', asset, '|', toString(decimals)) FROM reporting.fact_payment_events FINAL WHERE payment_id = '$paymentId'" `
        "payment.completed|100000000|USDT|6"
    Assert-Query "ledger entry index" `
        "SELECT toString(entry_index) FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = '$transactionId' AND account_id = 'acc-merchant'" "1"
    Assert-Query "ledger direction" `
        "SELECT direction FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = '$transactionId' AND account_id = 'acc-merchant'" "credit"
    Assert-Query "alert mapping" `
        "SELECT concat(alert_type, '|', severity) FROM reporting.fact_recon_alerts FINAL WHERE alert_id = '$alertId'" "amount_mismatch|warning"

    # Повторная доставка события схлопывается дедупликацией FINAL: дубль и
    # sentinel идут в одну партицию (общий ключ), ожидание sentinel доказывает
    # обработку дубля без наблюдения промежуточного состояния.
    Invoke-Producer @("-scenario", "duplicate", "-payment-id", $paymentId, "-event-id", $paymentEventId) | Out-Null
    $sentinelPaymentId = "pay-" + [Guid]::NewGuid().ToString()
    Invoke-Producer @("-scenario", "sentinel", "-payment-id", $sentinelPaymentId, "-key", $paymentId) | Out-Null
    Wait-Query "duplicate sentinel" `
        "SELECT count() FROM reporting.fact_payment_events FINAL WHERE payment_id = '$sentinelPaymentId'" "1"
    Assert-Query "payment dedup" `
        "SELECT count() FROM reporting.fact_payment_events FINAL WHERE payment_id = '$paymentId'" "1"

    # Битое сообщение не останавливает партицию: следующее валидное доходит.
    $broken = Invoke-Producer @("-scenario", "broken", "-payment-id", "pay-broken-$((New-Guid).ToString())")
    $afterBrokenId = Field-Value $broken "after_broken_payment_id"
    Wait-Query "after broken" `
        "SELECT count() FROM reporting.fact_payment_events FINAL WHERE payment_id = '$afterBrokenId'" "1"

    Write-Host "OK: migrations applied, pipeline reconciled (4 kafka tables, 4 views), full event flow green, values verified, duplicate deduped, broken message survived, readyz green"
} finally {
    Invoke-Compose @("--profile", "app", "down", "-v") | Out-Null
    Pop-Location
}
