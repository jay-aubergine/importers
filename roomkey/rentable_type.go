package roomkey

import (
	"encoding/csv"
	"fmt"
	"importers/core"
	"os"
	"path"
	"reflect"
	"rentroll/rlib"
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
// from roomkey csv file while avoiding duplicate FloorPlan/Style
func ReadRentableTypeCSVData(
	recordCount *int,
	rowIndex int,
	traceCSVData map[int]int,
	csvRow []string,
	avoidData *[]string,
	currentTime time.Time,
	suppliedValues map[string]string,
	rentableTypeStruct *core.RentableTypeCSV,
	business *rlib.Business,
	rentableTypeCSVData *[][]string,
	csvHeaderMap map[string]core.CSVHeader,
) {
	// get style
	checkRentableTypeStyle := csvRow[csvHeaderMap["RoomType"].Index]
	Stylefound := core.StringInSlice(checkRentableTypeStyle, *avoidData)

	// if style found then simplay return otherwise continue
	if Stylefound {
		return
	}

	*avoidData = append(*avoidData, checkRentableTypeStyle)

	currentYear, currentMonth, currentDate := currentTime.Date()
	DtStart := fmt.Sprintf("%d/%d/%d", currentMonth, currentDate, currentYear)
	DtStop := "12/31/9999" // no end date

	// make rentableType data from userSuppliedValues and defaultValues
	rentableTypeDefaultData := map[string]string{}
	for k, v := range suppliedValues {
		rentableTypeDefaultData[k] = v
	}
	rentableTypeDefaultData["DtStart"] = DtStart
	rentableTypeDefaultData["DtStop"] = DtStop

	// get csv row data
	csvRowData := GetRentableTypeCSVRow(
		csvRow, rentableTypeStruct,
		rentableTypeDefaultData,
		csvHeaderMap,
	)

	*rentableTypeCSVData = append(*rentableTypeCSVData, csvRowData)

	// after write operation to csv,
	// entry this rowindex with unit value in the map
	*recordCount = *recordCount + 1
	traceCSVData[*recordCount+1] = rowIndex

}

// GetRentableTypeCSVRow used to create rentabletype
// csv row from onesite csv
func GetRentableTypeCSVRow(
	roomkeyRow []string,
	fieldMap *core.RentableTypeCSV,
	DefaultValues map[string]string,
	csvHeaderMap map[string]core.CSVHeader,
) []string {

	// ======================================
	// Load rentableType's data from roomkeyRow data
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

		// get mapping field
		MappedFieldName := reflectedRentableTypeFieldMap.FieldByName(rentableTypeField.Name).Interface().(string)

		// if has not value then continue
		if header, ok := csvHeaderMap[MappedFieldName]; ok {
			dataMap[i] = roomkeyRow[header.Index]
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
