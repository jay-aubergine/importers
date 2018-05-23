package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"rentroll/rlib"
	"strconv"
	"strings"
)

// GetFieldMapping reads json file and loads
// field mapping structure in go for further usage
func GetFieldMapping(csvFieldMap *CSVFieldMap, mapperFilePath string) error {

	fieldmap, err := ioutil.ReadFile(mapperFilePath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(fieldmap, csvFieldMap)
	return err
}

// ValidateUserSuppliedValues validates all user supplied values
// return error list and also business unit
func ValidateUserSuppliedValues(ctx context.Context, userValues map[string]string) ([]error, *rlib.Business) {
	var errorList []error
	var accrualRateOptText = `| 0: one time only | 1: secondly | 2: minutely | 3: hourly | 4: daily | 5: weekly | 6: monthly | 7: quarterly | 8: yearly |`

	// --------------------- BUD validation ------------------------
	BUD := userValues["BUD"]
	business, err := rlib.GetBusinessByDesignation(ctx, BUD)
	if err != nil {
		errorList = append(errorList,
			fmt.Errorf("Supplied Business Unit Designation does not exists"))
	}
	// resource not found then consider it's as an error
	if business.BID == 0 {
		errorList = append(errorList,
			fmt.Errorf("Supplied Business Unit Designation does not exists"))
	}

	// --------------------- RentCycle validation ------------------------
	RentCycle, err := strconv.Atoi(userValues["RentCycle"])
	if err != nil || RentCycle < 0 || RentCycle > 8 {
		errorList = append(errorList,
			fmt.Errorf("Please, choose Frequency value from this\n%s", accrualRateOptText))
	}

	// --------------------- Proration validation ------------------------
	Proration, err := strconv.Atoi(userValues["Proration"])
	if err != nil || Proration < 0 || Proration > 8 {
		errorList = append(errorList,
			fmt.Errorf("Please, choose Proration value from this\n%s", accrualRateOptText))
	}

	// --------------------- GSRPC validation ------------------------
	GSRPC, err := strconv.Atoi(userValues["GSRPC"])
	if err != nil || GSRPC < 0 || GSRPC > 8 {
		errorList = append(errorList,
			fmt.Errorf("Please, choose GSRPC value from this\n%s", accrualRateOptText))
	}

	// finally return error list
	return errorList, &business
}

// GetCSVHeaders reads json file and loads
// CSVHeaders structure in go for further usage
func GetCSVHeaders(headerFilePath string) ([]CSVHeader, error) {

	csvHeaders := []CSVHeader{}

	headerMap, err := ioutil.ReadFile(headerFilePath)
	if err != nil {
		return csvHeaders, err
	}

	err = json.Unmarshal(headerMap, &csvHeaders)
	if err != nil {
		return csvHeaders, err
	}

	for key := range csvHeaders {
		csvHeaders[key].Index = -1
	}

	return csvHeaders, err
}

// StringInSlice used to check whether string a
// is present or not in slice list
func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

// IsValidEmail used to check valid email or not
func IsValidEmail(email string) bool {
	// TODO: confirm which regex to use
	// Re := regexp.MustCompile(`^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,4}$`)
	Re := regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+$`)
	return Re.MatchString(email)
}

// GetImportedCount get map of summaryCount as an argument
// then it hit db to get imported count for each type
func GetImportedCount(ctx context.Context, summaryCount map[int]map[string]int, BID int64) error {
	for dbType := range summaryCount {
		switch dbType {
		case DBCustomAttrRef:
			n, err := rlib.GetCountBusinessCustomAttrRefs(ctx, BID)
			if err != nil {
				return err
			}
			summaryCount[DBCustomAttrRef]["imported"] += n
			break
		case DBCustomAttr:
			n, err := rlib.GetCountBusinessCustomAttributes(ctx, BID)
			if err != nil {
				return err
			}
			summaryCount[DBCustomAttr]["imported"] += n
			break
		case DBRentableType:
			n, err := rlib.GetCountBusinessRentableTypes(ctx, BID)
			if err != nil {
				return err
			}
			summaryCount[DBRentableType]["imported"] += n
			break
		case DBPeople:
			n, err := rlib.GetCountBusinessTransactants(ctx, BID)
			if err != nil {
				return err
			}
			summaryCount[DBPeople]["imported"] += n
			break
		case DBRentable:
			n, err := rlib.GetCountBusinessRentables(ctx, BID)
			if err != nil {
				return err
			}
			summaryCount[DBRentable]["imported"] += n
			break
		case DBRentalAgreement:
			n, err := rlib.GetCountBusinessRentalAgreements(ctx, BID)
			if err != nil {
				return err
			}
			summaryCount[DBRentalAgreement]["imported"] += n
			break
		}
	}
	return nil
}

// DgtGrpSepToDgts converts separated group of digits string to
// plain digits string without any separator
// ex., 1,200,000 -> 1200000
func DgtGrpSepToDgts(dstr string) string {
	return strings.NewReplacer(",", "").Replace(dstr)
}
