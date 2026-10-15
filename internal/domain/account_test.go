package domain

import (
	"errors"
	"testing"
)

// TestDeriveAccountIDGoldenVectors проверяет воспроизведение всех golden-векторов
// плана счетов CBL-003, раздел 9: дрейф деривации между сервисами недопустим
// (KI-ledger-08).
func TestDeriveAccountIDGoldenVectors(t *testing.T) {
	cases := []struct {
		name        string
		accountType string
		asset       string
		decimals    uint8
		merchantRef string
		want        string
	}{
		{"clearing", "clearing", "USDT", 2, "", "dec27141-ec8f-55de-a325-04a35cd6b0f2"},
		{"fee", "fee", "USDT", 2, "", "b138b712-5940-59bc-82bd-19f653642227"},
		{"fee zero decimals", "fee", "USDT", 0, "", "8ee4265f-342c-5dbc-8d80-2516a5641b59"},
		{"suspense", "suspense", "USDT", 2, "", "cb2ccbda-92f9-5889-ba9f-c7ec90b8cfa0"},
		{"merchant", "merchant", "USDT", 2, "m-1", "58686062-c133-5d4e-b08b-0d33a0db924f"},
		{"merchant btc", "merchant", "BTC", 8, "m-1", "e1a671a1-a662-5305-b144-6e623099ea46"},
		{"merchant mixed ref", "merchant", "USDT", 2, "M-1_a", "00f2dc5b-a0c1-51c1-b2eb-f261e0ed538f"},
		{"payout", "payout", "USDT", 2, "m-1", "0595b42d-8f10-5bb5-b98f-a349594bc3f8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DeriveAccountID(tc.accountType, tc.asset, tc.decimals, tc.merchantRef)
			if err != nil {
				t.Fatalf("DeriveAccountID(%q, %q, %d, %q): %v", tc.accountType, tc.asset, tc.decimals, tc.merchantRef, err)
			}
			if got != tc.want {
				t.Errorf("DeriveAccountID(%q, %q, %d, %q) = %s, want %s",
					tc.accountType, tc.asset, tc.decimals, tc.merchantRef, got, tc.want)
			}
		})
	}
}

// TestDeriveAccountIDDeterministic проверяет повторяемость вывода.
func TestDeriveAccountIDDeterministic(t *testing.T) {
	first, err := DeriveAccountID("clearing", "USDT", 6, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := DeriveAccountID("clearing", "USDT", 6, "")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("derivation is not deterministic: %s != %s", first, second)
	}
}

// TestDeriveAccountIDInvalid проверяет отказ на ключах вне правил плана счетов.
func TestDeriveAccountIDInvalid(t *testing.T) {
	cases := []struct {
		name        string
		accountType string
		asset       string
		decimals    uint8
		merchantRef string
	}{
		{"unknown type", "reserve", "USDT", 2, ""},
		{"lowercase asset", "clearing", "usdt", 2, ""},
		{"empty asset", "clearing", "", 2, ""},
		{"long asset", "clearing", "TOOOOOOOOOOOOOOOLONG", 2, ""},
		{"platform with ref", "suspense", "USDT", 2, "m-1"},
		{"merchant without ref", "merchant", "USDT", 2, ""},
		{"merchant bad ref", "merchant", "USDT", 2, "m:1"},
		{"payout bad ref", "payout", "USDT", 2, "m 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DeriveAccountID(tc.accountType, tc.asset, tc.decimals, tc.merchantRef); !errors.Is(err, ErrInvalidAccountKey) {
				t.Errorf("DeriveAccountID(%q, %q, %d, %q) error = %v, want ErrInvalidAccountKey",
					tc.accountType, tc.asset, tc.decimals, tc.merchantRef, err)
			}
		})
	}
}

// TestValidAccountID проверяет форму идентификатора счета из Ledger.
func TestValidAccountID(t *testing.T) {
	valid := []string{
		"58686062-c133-5d4e-b08b-0d33a0db924f",
		"acc-merchant",
		"a",
		"0123456789012345678901234567890123456789012345678901234567890123",
	}
	for _, id := range valid {
		if !ValidAccountID(id) {
			t.Errorf("ValidAccountID(%q) = false, want true", id)
		}
	}
	invalid := []string{"", "acc'; DROP", "acc id", "acc:1", "01234567890123456789012345678901234567890123456789012345678901234"}
	for _, id := range invalid {
		if ValidAccountID(id) {
			t.Errorf("ValidAccountID(%q) = true, want false", id)
		}
	}
}

// TestValidAccountType проверяет замкнутое множество типов плана счетов.
func TestValidAccountType(t *testing.T) {
	for _, accountType := range append([]string{"merchant", "payout"}, PlatformAccountTypes...) {
		if !ValidAccountType(accountType) {
			t.Errorf("ValidAccountType(%q) = false, want true", accountType)
		}
	}
	for _, accountType := range []string{"", "reserve", "MERCHANT", "clearing "} {
		if ValidAccountType(accountType) {
			t.Errorf("ValidAccountType(%q) = true, want false", accountType)
		}
	}
}

// TestValidMerchantRef проверяет границы бизнес-ссылки мерчанта.
func TestValidMerchantRef(t *testing.T) {
	valid := []string{"m-1", "M-1_a", "m", "0", "a1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678"}
	for _, ref := range valid {
		if !ValidMerchantRef(ref) {
			t.Errorf("ValidMerchantRef(%q) = false, want true", ref)
		}
	}
	invalid := []string{"", "m:1", "m 1", "m/1", "а"}
	for _, ref := range invalid {
		if ValidMerchantRef(ref) {
			t.Errorf("ValidMerchantRef(%q) = true, want false", ref)
		}
	}
}
