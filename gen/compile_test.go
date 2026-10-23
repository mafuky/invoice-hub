package gen_test

import (
	"testing"

	invoicev1 "github.com/mafuky/invoice-hub/gen/invoicehub/invoice/v1"
	providerv1 "github.com/mafuky/invoice-hub/gen/invoicehub/provider/v1"
	settlementv1 "github.com/mafuky/invoice-hub/gen/invoicehub/settlement/v1"
)

func TestGeneratedServersCompile(t *testing.T) {
	var _ invoicev1.InvoiceServiceServer = invoicev1.UnimplementedInvoiceServiceServer{}
	var _ providerv1.ProviderServiceServer = providerv1.UnimplementedProviderServiceServer{}
	var _ settlementv1.SettlementServiceServer = settlementv1.UnimplementedSettlementServiceServer{}
}
