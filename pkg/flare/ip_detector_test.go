package flare

import (
	"testing"

	"github.com/octarinesec/secret-detector/pkg/detectors/tests"
)

var ipDetector = NewIPDetector()

var testCases = []tests.TestCase{
	{
		Name:            "ipv4",
		Input:           "192.168.0.1",
		ExpectDetection: true,
	},
	{
		Name:            "ipv6",
		Input:           "2001:db8:3333:4444:5555:6666:7777:8888",
		ExpectDetection: true,
	},
	{
		Name:            "ip_with_port",
		Input:           "192.168.0.1:8001",
		ExpectDetection: true,
	},
	{
		Name:            "partial_ipv6",
		Input:           "2001:db8:3333:4444",
		ExpectDetection: false,
	},
	{
		Name:            "looks_like_ip",
		Input:           "v192.168.0.1",
		ExpectDetection: false,
	},
	{
		Name:            "partial_ip",
		Input:           "192.168.0",
		ExpectDetection: false,
	},
	{
		Name:            "non_numerical_chars",
		Input:           "192.168.ABC.XYZ",
		ExpectDetection: false,
	},
	{
		Name:            "empty_input",
		Input:           "",
		ExpectDetection: false,
	},
}

func TestScan(t *testing.T) {
	tests.TestScan(t, ipDetector, testCases)
}

func TestScanWithKey(t *testing.T) {
	tests.TestScanWithKey(t, ipDetector, testCases)
}

func TestScanWithMultipleMatches(t *testing.T) {
	tests.TestScanWithMultipleMatches(t, ipDetector, testCases)
}
