// Package domain contains reporting rules that do not depend on infrastructure:
// derivation of chart-of-accounts identifiers for the dim_accounts dictionary.
package domain

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/google/uuid"
)

// accountNamePrefix sets the versioned prefix of the canonical account name:
// it mirrors the ledger chart of accounts for deterministic derivation of
// account_id (CBL-003, section 9; replication via golden vectors, KI-ledger-08).
const accountNamePrefix = "cbl:ledger:account:v1"

// assetPattern restricts the chart-of-accounts asset code.
var assetPattern = regexp.MustCompile(`^[A-Z0-9]{1,16}$`)

// merchantRefPattern restricts the merchant business reference (no colons).
var merchantRefPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// accountIDPattern restricts the form of the account identifier from Ledger (UUIDs
// and stand-compatible identifiers; it closes off strings that reach SQL).
var accountIDPattern = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)

// ErrInvalidAccountKey means a business key outside the chart-of-accounts rules.
var ErrInvalidAccountKey = errors.New("invalid account key")

// PlatformAccountTypes lists the system chart-of-accounts types whose derived
// records populate the dim_accounts dictionary.
var PlatformAccountTypes = []string{"clearing", "fee", "suspense"}

// DeriveAccountID derives a deterministic account UUIDv5 from the chart-of-accounts
// business key (CBL-003, section 9): the same tuple (type, asset, decimals,
// merchant_ref) yields the same identifier in any service. Replication is
// mandatory until the derivation moves to cbl-kit (KI-ledger-08).
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

// ValidAccountType reports whether the account type belongs to the chart of accounts.
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

// ValidAccountID reports whether the account identifier from Ledger has a valid
// form (it protects the dictionary from garbage values before writing to ClickHouse).
func ValidAccountID(accountID string) bool {
	return accountIDPattern.MatchString(accountID)
}

// ValidMerchantRef reports whether the merchant reference is allowed by the chart
// of accounts; it is used before writing the dictionary to ClickHouse.
func ValidMerchantRef(ref string) bool {
	return merchantRefPattern.MatchString(ref)
}

// ValidAsset reports whether the asset code is allowed by the chart of accounts.
func ValidAsset(asset string) bool {
	return assetPattern.MatchString(asset)
}
