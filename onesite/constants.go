package onesite

import "rentroll/rcsv"

// TempCSVStoreName holds the name of csvstore folder
var TempCSVStoreName = "temp_CSVs"

// TempCSVStore is used to store temporary csv files
var TempCSVStore string

var marketRent = "marketrent"

// FieldDefaultValues isused to overwrite if user has not passed to values for these fields
var FieldDefaultValues = map[string]string{
	"ManageToBudget": "1", // always take to default this one
	"RentCycle":      "6", // maybe overridden by user supplied value
	"Proration":      "4", // maybe overridden by user supplied value
	"GSRPC":          "4", // maybe overridden by user supplied value
	"AssignmentTime": "1", // always take to default this one
	"Renewal":        "2", // always take to default this one
}

// RentableUseStatusCSV is mapping for rentable status between onesite and rentroll
var RentableUseStatusCSV = map[string]string{
	"vacant":   "1",
	"occupied": "1",
	"model":    "7",
}

// prefixCSVFile is a map which holds the prefix of csv files
// so that temporarily program can create csv files with this
var prefixCSVFile = map[string]string{
	"rentable_types":   "rentableTypes_",
	"people":           "people_",
	"rental_agreement": "rentalAgreement_",
	"rentable":         "rentable_",
	"custom_attribute": "customAttribute_",
}

// CSVLoadHandler struct is for routines that want to table-ize their loading.
type csvLoadHandler struct {
	Fname        string
	Handler      rcsv.CSVLoadHandlerFunc
	TraceDataMap string
	DBType       int
}

// CARD Custom Attriute Ref Data struct, holds data
// from which we'll insert customAttributeRef in system
type CARD struct {
	BID      int64
	RTID     string
	Style    string
	SqFt     int64
	CID      string
	RowIndex int
}

// will be used exact before rowIndex to format Notes in people csv "onesite:<rowIndex>"
const (
	onesiteNotesPrefix = "onesite$"
	tcidPrefix         = "TC000"
)

// this slice contains list of strings which should be discarded
// used in csvRecordsToSkip function
var csvRecordsSkipList = []string{
	rcsv.DupTransactant,
	rcsv.DupRentableType,
	rcsv.DupCustomAttribute,
	rcsv.DupRentable,
	rcsv.RentableAlreadyRented,
}

var dupTransactantWithPrimaryEmail = "PrimaryEmail"
