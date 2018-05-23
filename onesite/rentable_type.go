package onesite

import (
	"encoding/csv"
	"fmt"
	"importers/core"
	"os"
	"path"
	"reflect"
	"rentroll/rlib"
	"strconv"
	"time"
)

// CreateRentableTypeCSV create rentabletype csv temporarily
// write headers, used to load data from onesite csv
// return file pointer to call program
func CreateRentableTypeCSV(
	CSVStore string,
	timestamp string,
	rt *core.RentableTypeCSV,
) (*os.File, *csv.Writer, bool) {

	var done = false

	// get path of rentable csv file
	filePrefix := prefixCSVFile["rentable_types"]
	fileName := filePrefix + timestamp + ".csv"
	rentableTypeCSVFilePath := path.Join(CSVStore, fileName)

	// try to create file and return with error if occurs any
	rentableTypeCSVFile, err := os.Create(rentableTypeCSVFilePath)
	if err != nil {
		rlib.Ulog("Error <RENTABLE TYPE CSV>: %s\n", err.Error())
		return nil, nil, done
	}

	// create csv writer
	rentableTypeCSVWriter := csv.NewWriter(rentableTypeCSVFile)

	// parse headers of rentableTypeCSV using reflect
	rentableTypeCSVHeaders, ok := core.GetStructFields(rt)
	if !ok {
		rlib.Ulog("Error <RENTABLE TYPE CSV>: Unable to get struct fields for rentableTypeCSV\n")
		return nil, nil, done
	}

	rentableTypeCSVWriter.Write(rentableTypeCSVHeaders)
	rentableTypeCSVWriter.Flush()

	done = true

	return rentableTypeCSVFile, rentableTypeCSVWriter, done
}

// ReadRentableTypeCSVData used to read the data for RentableType csv
// from onesite csv file while avoiding duplicate FloorPlan/Style
func ReadRentableTypeCSVData(
	recordCount *int,
	rowIndex int,
	traceCSVData map[int][]int,
	csvRow []string,
	rentableTypeCSVData *[][]string,
	avoidData *[]string,
	currentTime time.Time,
	suppliedValues map[string]string,
	rentableTypeStruct *core.RentableTypeCSV,
	customAttributesRefData map[string]CARD,
	csvHeaderMap map[string]core.CSVHeader,
	business *rlib.Business,
) {
	// get style from the onesite row
	rentableTypeStyle := csvRow[csvHeaderMap["FloorPlan"].Index]

	// check if style is already present or not
	Stylefound := core.StringInSlice(rentableTypeStyle, *avoidData)

	// if style found then simply return otherwise continue
	if Stylefound {
		return
	}
	// add style to avoidData
	*avoidData = append(*avoidData, rentableTypeStyle)

	// insert CARD for this style in customAttributesRefData
	// no need to verify err, it has been passed already
	// through first loop in main program
	sqft, _ := strconv.ParseInt(csvRow[csvHeaderMap["SQFT"].Index], 10, 64)
	tempCard := CARD{
		//currently taking 1 as BID because we are not passing rlib.Business
		BID:      business.BID,
		Style:    rentableTypeStyle,
		SqFt:     sqft,
		RowIndex: rowIndex,
	}
	customAttributesRefData[rentableTypeStyle] = tempCard

	currentYear, currentMonth, currentDate := currentTime.Date()
	DtStart := fmt.Sprintf("%d/%d/%d", currentMonth, currentDate, currentYear)

	DtStop := "12/31/9999" // no end date

	// make rentableType data from userSuppliedValues and defaultValues
	rentableTypeDefaultData := map[string]string{}

	//currently ignoring as no default data supplied
	for k, v := range suppliedValues {
		rentableTypeDefaultData[k] = v
	}

	// rentableTypeDefaultData["RentCycle"] = "6"
	// rentableTypeDefaultData["Proration"] = "4"
	// rentableTypeDefaultData["GSRPC"] = "4"
	// rentableTypeDefaultData["BUD"] = "ISO"
	// rentableTypeDefaultData["ManageToBudget"] = "1"

	rentableTypeDefaultData["DtStart"] = DtStart
	rentableTypeDefaultData["DtStop"] = DtStop

	// get csv row data
	csvRowData := GetRentableTypeCSVRow(
		csvRow, rentableTypeStruct,
		rentableTypeDefaultData,
		csvHeaderMap,
	)

	*rentableTypeCSVData = append(*rentableTypeCSVData, csvRowData)

	*recordCount = *recordCount + 1

	// need to map on next row index of temp csv as first row is header line
	// and recordCount initialized with 0 value
	// traceCSVData[*recordCount+1] = rowIndex + 1
	traceCSVData[*recordCount+1] = append(traceCSVData[*recordCount+1], rowIndex+1)

}

// GetRentableTypeCSVRow used to create rentabletype
// csv row from onesite csv
func GetRentableTypeCSVRow(
	oneSiteRow []string,
	fieldMap *core.RentableTypeCSV,
	DefaultValues map[string]string,
	csvHeaderMap map[string]core.CSVHeader,
) []string {

	// ======================================
	// Load rentableType's data from onesiterow data
	// ======================================
	reflectedRentableTypeFieldMap := reflect.ValueOf(fieldMap).Elem()

	// length of RentableTypeCSV
	rRTLength := reflectedRentableTypeFieldMap.NumField()

	// return data array
	dataMap := make(map[int]string)

	for i := 0; i < rRTLength; i++ {
		// get rentableType field
		rentableTypeField := reflectedRentableTypeFieldMap.Type().Field(i)

		// if rentableTypeField value exist in DefaultValues map
		// then set it first
		suppliedValue, found := DefaultValues[rentableTypeField.Name]
		if found {
			dataMap[i] = suppliedValue
		}

		// get mapping field if not found then panic error
		MappedFieldName := reflectedRentableTypeFieldMap.FieldByName(rentableTypeField.Name).Interface().(string)

		// if has not value then continue
		if header, ok := csvHeaderMap[MappedFieldName]; ok {
			dataMap[i] = oneSiteRow[header.Index]
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
