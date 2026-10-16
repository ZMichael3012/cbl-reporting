# E2E CBL-11.4: gRPC-сервис отчетов ReportingService.Query на compose-стенде.
# Запуск из каталога сервиса: powershell -NoProfile -File test/e2e/reports.ps1
# Сценарий: compose up (Kafka + ClickHouse + ledger-stub + сервис) -> факты ->
# словарь и backfill -> grpcurl Query: payments, пагинация по курсору,
# ledger_entries с мерчантским фильтром, turnover, unknown type и invalid
# group_by -> down -v.
$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$ComposeFile = Join-Path $RepoRoot "deploy\docker-compose.yml"
$Network = "cbl-reporting_cbl-net"
$ProtosDir = Resolve-Path (Join-Path $RepoRoot "..\cbl-protos\proto")

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

# Invoke-Query вызывает ReportingService.Query через grpcurl в сети стенда и
# возвращает разобранный JSON; тело передается через stdin (`-d @`): PowerShell
# съедает кавычки inline-JSON при вызове нативных команд.
function Invoke-Query([string]$Body) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = $Body | docker run -i --rm --network $Network -v "${ProtosDir}:/p" `
            fullstorydev/grpcurl:v1.9.1 `
            -plaintext -import-path /p -proto billing/reporting/v1/reporting.proto `
            -d "@" reporting:9110 billing.reporting.v1.ReportingService/Query 2>&1
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    return @{ Output = ($output -join "`n"); Code = $code }
}

# Assert-QueryError проверяет отказ Query с ожидаемым кодом gRPC.
function Assert-QueryError([string]$Name, [string]$Body, [string]$Code) {
    $result = Invoke-Query $Body
    if ($result.Code -eq 0) { throw "$Name`: ожидалась ошибка $Code, пришел успех: $($result.Output)" }
    if ($result.Output -notmatch $Code) { throw "$Name`: ожидался код $Code, вывод: $($result.Output)" }
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
    $up = Invoke-Compose @("up", "-d", "--build")
    if ($up.Code -ne 0) { throw "infra up failed: $($up.Output)" }

    foreach ($attempt in 1..40) {
        $health = Invoke-Compose @("ps", "--format", "{{.Service}} {{.Health}}")
        if ($health.Output -notmatch "starting") { break }
        Start-Sleep -Seconds 2
    }

    foreach ($topic in @("payment.events", "ledger.events", "payout.events", "recon.alerts")) {
        $create = Invoke-Compose @("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "localhost:9092", "--create", "--topic", $topic,
            "--partitions", "1", "--replication-factor", "1")
        if ($create.Code -ne 0) { throw "topic $topic failed: $($create.Output)" }
    }

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

    # Два полных потока: два платежа и две проводки у мерчанта m-1.
    $first = Invoke-Producer @("-scenario", "full")
    $firstPaymentId = Field-Value $first "payment_id"
    $firstTransactionId = Field-Value $first "transaction_id"
    $second = Invoke-Producer @("-scenario", "full")
    $secondPaymentId = Field-Value $second "payment_id"
    $secondTransactionId = Field-Value $second "transaction_id"

    Wait-Query "payment rows" `
        "SELECT count() FROM reporting.fact_payment_events FINAL WHERE merchant_id = 'm-1'" "2"
    Wait-Query "ledger rows" `
        "SELECT count() FROM reporting.fact_ledger_entries FINAL WHERE account_id = 'acc-merchant' AND merchant_id = 'm-1'" "2"

    # payments: строки мерчанта, канонические деньги и фиксированное время.
    $page = Invoke-Query '{"report_type":"payments","merchant_id":"m-1","limit":20}'
    if ($page.Code -ne 0) { throw "payments query failed: $($page.Output)" }
    $parsed = $page.Output | ConvertFrom-Json
    if ($parsed.rows.Count -ne 2) { throw "payments: ожидалось 2 строки, получено $($parsed.rows.Count)" }
    if ($parsed.rows[0].values.expected_amount -ne "100.000000" -or $parsed.rows[0].values.asset -ne "USDT") {
        throw "payments: значения не совпали: $($parsed.rows[0] | ConvertTo-Json -Compress)"
    }
    if ($parsed.rows[0].values.occurred_at -notlike "2026-10-15T10:00:00*") {
        throw "payments: время не совпало: $($parsed.rows[0].values.occurred_at)"
    }

    # Пагинация: limit=1 -> курсор -> вторая страница -> конец отчета.
    $firstPage = Invoke-Query '{"report_type":"payments","merchant_id":"m-1","limit":1}'
    if ($firstPage.Code -ne 0) { throw "pagination page1 failed: $($firstPage.Output)" }
    $pageOne = $firstPage.Output | ConvertFrom-Json
    if ($pageOne.rows.Count -ne 1 -or [string]::IsNullOrEmpty($pageOne.nextCursor)) {
        throw "пагинация: первая страница без курсора: $($firstPage.Output)"
    }
    $secondPage = Invoke-Query (ConvertTo-Json -Compress @{
        report_type = "payments"; merchant_id = "m-1"; limit = 1; cursor = $pageOne.nextCursor
    } -Depth 3)
    if ($secondPage.Code -ne 0) { throw "pagination page2 failed: $($secondPage.Output)" }
    $pageTwo = $secondPage.Output | ConvertFrom-Json
    if ($pageTwo.rows.Count -ne 1 -or -not [string]::IsNullOrEmpty($pageTwo.nextCursor)) {
        throw "пагинация: конец отчета не достигнут: $($secondPage.Output)"
    }

    # ledger_entries: мерчантский фильтр по денормализации (acc-clearing без
    # словарной записи мерчанту не виден).
    $ledger = Invoke-Query '{"report_type":"ledger_entries","merchant_id":"m-1","limit":20}'
    if ($ledger.Code -ne 0) { throw "ledger query failed: $($ledger.Output)" }
    $ledgerRows = ($ledger.Output | ConvertFrom-Json).rows
    if ($ledgerRows.Count -ne 2) { throw "ledger_entries: ожидалось 2 строки, получено $($ledgerRows.Count)" }
    foreach ($row in $ledgerRows) {
        if ($row.values.account_id -ne "acc-merchant" -or $row.values.account_type -ne "merchant") {
            throw "ledger_entries: чужая строка: $($row | ConvertTo-Json -Compress)"
        }
    }

    # turnover: агрегат оборота мерчанта по активу.
    $turnover = Invoke-Query '{"report_type":"turnover","merchant_id":"m-1","group_by":"asset"}'
    if ($turnover.Code -ne 0) { throw "turnover query failed: $($turnover.Output)" }
    $turnoverRows = ($turnover.Output | ConvertFrom-Json).rows
    if ($turnoverRows.Count -ne 1 -or $turnoverRows[0].values.asset -ne "USDT") {
        throw "turnover: неожиданные строки: $($turnover.Output)"
    }
    if ($turnoverRows[0].values.credit_total -ne "200.000000" -or $turnoverRows[0].values.entries_count -ne "2") {
        throw "turnover: значения не совпали: $($turnoverRows[0] | ConvertTo-Json -Compress)"
    }

    # Ошибки каталога и параметров.
    Assert-QueryError "unknown type" '{"report_type":"invoices"}' "NotFound"
    Assert-QueryError "invalid group_by" '{"report_type":"payments","group_by":"asset"}' "InvalidArgument"

    Write-Host "OK: reports query green (payments values, cursor pagination, merchant-filtered ledger entries, turnover aggregate, NotFound/InvalidArgument)"
} finally {
    Invoke-Compose @("--profile", "app", "down", "-v") | Out-Null
    Pop-Location
}
