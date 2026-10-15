// Пакет domain содержит правила отчетности, не зависящие от инфраструктуры:
// деривацию идентификаторов счетов плана счетов для словаря dim_accounts.
package domain

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/google/uuid"
)

// accountNamePrefix задает версионированный префикс канонического имени счета:
// повторяет план счетов леджера для детерминированного вывода account_id
// (CBL-003, раздел 9; репликация по golden-векторам, KI-ledger-08).
const accountNamePrefix = "cbl:ledger:account:v1"

// assetPattern ограничивает код актива плана счетов.
var assetPattern = regexp.MustCompile(`^[A-Z0-9]{1,16}$`)

// merchantRefPattern ограничивает бизнес-ссылку мерчанта (без двоеточий).
var merchantRefPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// accountIDPattern ограничивает форму идентификатора счета из Ledger (UUID и
// совместимые идентификаторы стенда; замыкает строки, попадающие в SQL).
var accountIDPattern = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)

// ErrInvalidAccountKey означает бизнес-ключ вне правил плана счетов.
var ErrInvalidAccountKey = errors.New("invalid account key")

// PlatformAccountTypes перечисляет системные типы счетов плана счетов,
// производные записи которых наполняет словарь dim_accounts.
var PlatformAccountTypes = []string{"clearing", "fee", "suspense"}

// DeriveAccountID выводит детерминированный UUIDv5 счета из бизнес-ключа
// плана счетов (CBL-003, раздел 9): одинаковая пара (тип, актив, шкала,
// merchant_ref) в любом сервисе дает один и тот же идентификатор. Репликация
// обязательна до переноса вывода в cbl-kit (KI-ledger-08).
func DeriveAccountID(accountType, asset string, decimals uint8, merchantRef string) (string, error) {
	personal := accountType == "merchant" || accountType == "payout"
	platform := false
	for _, candidate := range PlatformAccountTypes {
		if accountType == candidate {
			platform = true
			break
		}
	}
	if !personal && !platform {
		return "", fmt.Errorf("%w: unknown account type %q", ErrInvalidAccountKey, accountType)
	}
	if !assetPattern.MatchString(asset) {
		return "", fmt.Errorf("%w: asset %q", ErrInvalidAccountKey, asset)
	}
	if decimals > 18 {
		return "", fmt.Errorf("%w: decimals %d", ErrInvalidAccountKey, decimals)
	}
	if personal && !merchantRefPattern.MatchString(merchantRef) {
		return "", fmt.Errorf("%w: %s account requires merchant_ref", ErrInvalidAccountKey, accountType)
	}
	if platform && merchantRef != "" {
		return "", fmt.Errorf("%w: %s account is platform-level", ErrInvalidAccountKey, accountType)
	}
	name := fmt.Sprintf("%s:%s:%s:%d:%s", accountNamePrefix, accountType, asset, decimals, merchantRef)
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(name)).String(), nil
}

// ValidAccountType сообщает, что тип счета входит в план счетов.
func ValidAccountType(accountType string) bool {
	if accountType == "merchant" || accountType == "payout" {
		return true
	}
	for _, candidate := range PlatformAccountTypes {
		if accountType == candidate {
			return true
		}
	}
	return false
}

// ValidAccountID сообщает, что идентификатор счета из Ledger имеет допустимую
// форму (защита словаря от мусорных значений до записи в ClickHouse).
func ValidAccountID(accountID string) bool {
	return accountIDPattern.MatchString(accountID)
}

// ValidMerchantRef сообщает, что ссылка на мерчанта допустима планом счетов;
// используется перед записью словаря в ClickHouse.
func ValidMerchantRef(ref string) bool {
	return merchantRefPattern.MatchString(ref)
}

// ValidAsset сообщает, что код актива допустим планом счетов.
func ValidAsset(asset string) bool {
	return assetPattern.MatchString(asset)
}
