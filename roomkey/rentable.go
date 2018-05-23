package roomkey

import (
	"encoding/csv"
	"fmt"
	"importers/core"
	"os"
	"path"
	"reflect"
	"rentroll/rlib"
	"strings"
	"time"
)

// CreateRentableCSV create rentable csv temporarily
// write headers, used to load data from roomkey csv
// return file pointer to call program
func CreateRentableCSV(
	CSVStore string,
	timestamp string,
	rentableStruct *core.RentableCSV,
) (*os.File, *csv.Writer, bool) {

	var done = false

	// get path of rentable csv file
	filePrefix := prefixCSVFile["rentable"]
	fileName := filePrefix + timestamp + ".csv"
	rentableCSVFilePath := path.Join(CSVStore, fileName)

	// try to create file and return with error if occurs any
	rentableCSVFile, err := os.Create(rentableCSVFilePath)
	if err != nil {
		rlib.Ulog("Error <RENTABLE CSV>: %s\n", err.Error())
		return nil, nil, done
	}

	// create csv writer
	rentableCSVWriter := csv.NewWriter(rentableCSVFile)

	// parse headers of rentableCSV using reflect
	rentableCSVHeaders, ok := core.GetStructFields(rentableStruct)
	if !ok {
		rlib.Ulog("Error <RENTABLE CSV>: Unable to get struct fields for rentableCSV\n")
		return nil, nil, done
	}

	rentableCSVWriter.Write(rentableCSVHeaders)
	rentableCSVWriter.Flush()

	done = true

	return rentableCSVFile, rentableCSVWriter, done
}

// ReadRentableCSVData used to read the data for Rentable csv
// from roomkey csv file while avoiding duplicate data
func ReadRentableCSVData(
	recordCount *int,
	rowIndex int,
	traceCSVData map[int]int,
	csvRow []string,
	currentTime time.Time,
	suppliedValues map[string]string,
	rentableStruct *core.RentableCSV,
	traceTCIDMap map[int]string,
	csvErrors map[int][]string,
	rentableCSVData *[][]string,
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

	// Forming default rentable status string
	rentableDefaultData["DtStart"] = DtStart
	rentableDefaultData["DtStop"] = DtStop
	rentableDefaultData["TCID"] = traceTCIDMap[rowIndex]

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
			warnPrefix+"No lease end date found. Using default value: "+DtStop,
		)
	}

	// get csv row data
	csvRowData := GetRentableCSVRow(
		csvRow, rentableStruct,
		rentableDefaultData,
		csvHeaderMap,
	)

	*rentableCSVData = append(*rentableCSVData, csvRowData)

	// after write operation to csv,
	// entry this rowindex with unit value in the map
	*recordCount = *recordCount + 1
	traceCSVData[*recordCount+1] = rowIndex
}

// GetRentableCSVRow used to create rentabletype
// csv row from roomkey csv
func GetRentableCSVRow(
	roomkeyRow []string,
	fieldMap *core.RentableCSV,
	DefaultValues map[string]string,
	csvHeaderMap map[string]core.CSVHeader,
) []string {

	// ======================================
	// Load rentable's data from roomkeyrow data
	// ======================================
	reflectedRentableFieldMap := reflect.ValueOf(fieldMap).Elem()

	// length of RentableCSV
	rRTLength := reflectedRentableFieldMap.NumField()

	// return data array
	dataMap := make(map[int]string)

	for i := 0; i < rRTLength; i++ {
		// get rentable field
		rentableField := reflectedRentableFieldMap.Type().Field(i)

		// if rentableField value exist in DefaultValues map
		// then set it first
		suppliedValue, found := DefaultValues[rentableField.Name]
		if found {
			dataMap[i] = strings.TrimSpace(suppliedValue)
		}

		// =========================================================
		// this condition has been put here because it's mapping field does not exist
		// =========================================================
		if rentableField.Name == "RentableTypeRef" {
			dataMap[i] = GetRentableTypeRef(roomkeyRow, DefaultValues, csvHeaderMap)
		}
		if rentableField.Name == "RUserSpec" {
			// format is user, startDate, stopDate
			dataMap[i] = GetRUserSpec(roomkeyRow, DefaultValues, csvHeaderMap)
		}
		if rentableField.Name == "RentableStatus" {
			// format is status, startDate, stopDate
			status := GetRentableStatus(roomkeyRow, DefaultValues, csvHeaderMap)
			// TODO: verify that what to do in false case
			// should return its original value or raise error???
			dataMap[i] = status
		}

		// get mapping field
		MappedFieldName := reflectedRentableFieldMap.FieldByName(rentableField.Name).Interface().(string)

		// if has not value then continue
		if header, ok := csvHeaderMap[MappedFieldName]; ok {
			dataMap[i] = strings.TrimSpace(roomkeyRow[header.Index])
		} else {
			continue
		}
	}

	dataArray := []string{}

	for i := 0; i < rRTLength; i++ {
		dataArray = append(dataArray, dataMap[i])
	}

	return dataArray
}

// GetRUserSpec used to get ruser spec in format of rentroll system
func GetRUserSpec(
	csvRow []string,
	defaults map[string]string,
	csvHeaderMap map[string]core.CSVHeader,
) string {

	// always it is ONLINE then leave it as a blank
	// as rcsv loader automatically associate user from rental
	// agreement csv so leave it as blank (nearly all cases)
	return ""
}

// GetRentableStatus used to get rentable status in format of rentroll system
func GetRentableStatus(
	csvRow []string,
	defaults map[string]string,
	csvHeaderMap map[string]core.CSVHeader,
) string {

	orderedFields := []string{}

	// rentable status is always online then
	// append unitleasestatus
	orderedFields = append(orderedFields, RoomKeyOnlineRentableStatus)

	// append today start date
	orderedFields = append(orderedFields, defaults["DtStart"])

	// append end date unspecified
	orderedFields = append(orderedFields, "")

	return strings.Join(orderedFields, ",")

}

// GetRentableTypeRef used to get rentable type ref in format of rentroll system
func GetRentableTypeRef(
	csvRow []string,
	defaults map[string]string,
	csvHeaderMap map[string]core.CSVHeader,
) string {

	orderedFields := []string{}

	// append floor plan
	orderedFields = append(orderedFields, strings.TrimSpace(csvRow[csvHeaderMap["RoomType"].Index]))

	// append today date
	orderedFields = append(orderedFields, defaults["DtStart"])

	// append end date as unspecified
	orderedFields = append(orderedFields, "")

	return strings.Join(orderedFields, ",")
}
