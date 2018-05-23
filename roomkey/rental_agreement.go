package roomkey

import (
	"encoding/csv"
	"fmt"
	"importers/core"
	"os"
	"path"
	"reflect"
	"rentroll/rlib"
	"strconv"
	"strings"
	"time"
)

// CreateRentalAgreementCSV create rental agreement csv temporarily
// write headers, used to load data from onesite csv
// return file pointer to call program
func CreateRentalAgreementCSV(
	CSVStore string,
	timestamp string,
	rentalAgreementStruct *core.RentalAgreementCSV,
) (*os.File, *csv.Writer, bool) {

	var done = false

	// get path of rentalAgreement csv file
	filePrefix := prefixCSVFile["rental_agreement"]
	fileName := filePrefix + timestamp + ".csv"
	rentalAgreementCSVFilePath := path.Join(CSVStore, fileName)

	// try to create file and return with error if occurs any
	rentalAgreementCSVFile, err := os.Create(rentalAgreementCSVFilePath)
	if err != nil {
		rlib.Ulog("Error <RENTAL AGREEMENT CSV>: %s\n", err.Error())
		return nil, nil, done
	}

	// create csv writer
	rentalAgreementCSVWriter := csv.NewWriter(rentalAgreementCSVFile)

	// parse headers of rentalAgreementCSV using reflect
	rentalAgreementCSVHeaders, ok := core.GetStructFields(rentalAgreementStruct)
	if !ok {
		rlib.Ulog("Error <RENTAL AGREEMENT CSV>: Unable to get struct fields for rentalAgreementCSV\n")
		return nil, nil, done
	}

	rentalAgreementCSVWriter.Write(rentalAgreementCSVHeaders)
	rentalAgreementCSVWriter.Flush()

	done = true

	return rentalAgreementCSVFile, rentalAgreementCSVWriter, done
}

// ReadRentalAgreementCSVData used to read the data for RentalAggrement csv
// from roomkey csv file while avoiding duplicate data
func ReadRentalAgreementCSVData(
	recordCount *int,
	rowIndex int,
	traceCSVData map[int]int,
	csvRow []string,
	currentTime time.Time,
	suppliedValues map[string]string,
	rentalAgreementStruct *core.RentalAgreementCSV,
	traceTCIDMap map[int]string,
	csvErrors map[int][]string,
	rentalAgreementCSVData *[][]string,
	csvHeaderMap map[string]core.CSVHeader,
) {

	currentYear, currentMonth, currentDate := currentTime.Date()
	DtStart := fmt.Sprintf("%d/%d/%d", currentMonth, currentDate, currentYear)
	DtStop := "12/31/9999" // no end date

	// make rentable data from userSuppliedValues and defaultValues
	rentableDefaultData := map[string]string{}
	for k, v := range suppliedValues {
		rentableDefaultData[k] = v
	}

	// flag warning that we are taking default values for least start, end dates
	// as they don't exists
	if csvRow[csvHeaderMap["DateIn"].Index] == "" {
		warnPrefix := "W:<" + core.DBTypeMapStrings[core.DBRentable] + ">:"
		csvErrors[rowIndex] = append(csvErrors[rowIndex],
			warnPrefix+"No lease start date found. Using default value: "+DtStart,
		)
	}
	if csvRow[csvHeaderMap["DateOut"].Index] == "" {
		warnPrefix := "W:<" + core.DBTypeMapStrings[core.DBRentable] + ">:"
		csvErrors[rowIndex] = append(csvErrors[rowIndex],
			warnPrefix+"No lease start date found. Using default value: "+DtStop,
		)
	}

	rentableDefaultData["DtStart"] = DtStart
	rentableDefaultData["DtStop"] = DtStop
	rentableDefaultData["TCID"] = traceTCIDMap[rowIndex]

	// get csv row data
	csvRowData := GetRentalAgreementCSVRow(
		csvRow, rentalAgreementStruct,
		rentableDefaultData, currentTime,
		csvHeaderMap,
	)

	*rentalAgreementCSVData = append(*rentalAgreementCSVData, csvRowData)

	// entry this rowindex with unit value in the map
	*recordCount = *recordCount + 1
	traceCSVData[*recordCount+1] = rowIndex
}

// GetRentalAgreementCSVRow used to create RentalAgreement
// csv row from roomkey csv
func GetRentalAgreementCSVRow(
	roomkeyRow []string,
	fieldMap *core.RentalAgreementCSV,
	DefaultValues map[string]string,
	currentTime time.Time,
	csvHeaderMap map[string]core.CSVHeader,
) []string {

	// ======================================
	// Load rentalAgreement's data from roomkeyrow data
	// ======================================
	reflectedRentalAgreementFieldMap := reflect.ValueOf(fieldMap).Elem()

	// length of RentalAgreementCSV
	rRTLength := reflectedRentalAgreementFieldMap.NumField()

	// return data array
	dataMap := make(map[int]string)

	for i := 0; i < rRTLength; i++ {
		// get rentalAgreement field
		rentalAgreementField := reflectedRentalAgreementFieldMap.Type().Field(i)

		// if rentalAgreementField value exist in DefaultValues map
		// then set it first
		suppliedValue, found := DefaultValues[rentalAgreementField.Name]
		if found {
			dataMap[i] = suppliedValue
		}

		// =========================================================
		// this condition has been put here because it's mapping field does not exist
		// =========================================================
		if rentalAgreementField.Name == "PayorSpec" {
			dataMap[i] = getPayorSpec(roomkeyRow, DefaultValues, csvHeaderMap)
		}
		if rentalAgreementField.Name == "UserSpec" {
			dataMap[i] = getUserSpec(roomkeyRow, DefaultValues, csvHeaderMap)
		}
		if rentalAgreementField.Name == "RentableSpec" {
			dataMap[i] = getRentableSpec(roomkeyRow, csvHeaderMap)
		}

		// get mapping field
		MappedFieldName := reflectedRentalAgreementFieldMap.FieldByName(rentalAgreementField.Name).Interface().(string)

		// if has not value then continue
		if header, ok := csvHeaderMap[MappedFieldName]; ok {
			dataMap[i] = roomkeyRow[header.Index]
		} else {
			continue
		}

		// Formatting dates to RentRoll importable format
		if rentalAgreementField.Name == "AgreementStart" {

			if len(strings.TrimSpace(roomkeyRow[csvHeaderMap["DateRes"].Index])) < 11 {
				currentYear, _, _ := currentTime.Date()
				dataMap[i] = getFormattedDate(strings.TrimSpace(roomkeyRow[csvHeaderMap["DateRes"].Index]) + strconv.Itoa(currentYear))
			} else {
				dataMap[i] = getFormattedDate(strings.TrimSpace(roomkeyRow[csvHeaderMap["DateRes"].Index]))
			}
		}
		if rentalAgreementField.Name == "PossessionStart" ||
			rentalAgreementField.Name == "RentStart" {
			dataMap[i] = getFormattedDate(strings.TrimSpace(roomkeyRow[csvHeaderMap["DateIn"].Index]))
		}
		if rentalAgreementField.Name == "AgreementStop" ||
			rentalAgreementField.Name == "PossessionStop" ||
			rentalAgreementField.Name == "RentStop" {
			dataMap[i] = getFormattedDate(strings.TrimSpace(roomkeyRow[csvHeaderMap["DateOut"].Index]))
		}

	}

	dataArray := []string{}

	for i := 0; i < rRTLength; i++ {
		dataArray = append(dataArray, dataMap[i])
	}

	return dataArray
}

// getPayorSpec used to get payor spec in format of rentroll system
func getPayorSpec(
	csvRow []string,
	defaults map[string]string,
	csvHeaderMap map[string]core.CSVHeader,
) string {

	orderedFields := []string{}

	// append TCID for user identification
	orderedFields = append(orderedFields, defaults["TCID"])

	if defaults["TCID"] != "" {
		// append rent start
		if csvRow[csvHeaderMap["DateIn"].Index] == "" {
			orderedFields = append(orderedFields, defaults["DtStart"])
		} else {
			orderedFields = append(orderedFields, getFormattedDate(csvRow[csvHeaderMap["DateIn"].Index]))
		}

		// append date out
		if csvRow[csvHeaderMap["DateOut"].Index] == "" {
			orderedFields = append(orderedFields, defaults["DtStop"])
		} else {
			orderedFields = append(orderedFields, getFormattedDate(csvRow[csvHeaderMap["DateOut"].Index]))
		}
	}

	return strings.Join(orderedFields, ",")
}

// getUserSpec used to get user spec in format of rentroll system
func getUserSpec(
	csvRow []string,
	defaults map[string]string,
	csvHeaderMap map[string]core.CSVHeader,
) string {

	orderedFields := []string{}

	orderedFields = append(orderedFields, defaults["TCID"])

	if defaults["TCID"] != "" {
		// append rent start
		if csvRow[csvHeaderMap["DateIn"].Index] == "" {
			orderedFields = append(orderedFields, defaults["DtStart"])
		} else {
			orderedFields = append(orderedFields, getFormattedDate(csvRow[csvHeaderMap["DateIn"].Index]))
		}

		// append date out
		if csvRow[csvHeaderMap["DateOut"].Index] == "" {
			orderedFields = append(orderedFields, defaults["DtStop"])
		} else {
			orderedFields = append(orderedFields, getFormattedDate(csvRow[csvHeaderMap["DateOut"].Index]))
		}
	}

	return strings.Join(orderedFields, ",")
}

// getRentableSpec used to get rentable spec in format of rentroll system
func getRentableSpec(
	csvRow []string,
	csvHeaderMap map[string]core.CSVHeader,
) string {

	orderedFields := []string{}

	// append rentable
	orderedFields = append(orderedFields, csvRow[csvHeaderMap["Room"].Index])
	// append contractrent
	rent := csvRow[csvHeaderMap["Rate"].Index]
	rent = strings.Replace(rent, "$", "", -1)
	// rent = strings.Replace(rent, ".", "", -1)
	orderedFields = append(orderedFields, rent)

	return strings.Join(orderedFields, ",")
}
