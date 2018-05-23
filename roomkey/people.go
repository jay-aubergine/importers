package roomkey

import (
	"encoding/csv"
	"importers/core"
	"os"
	"path"
	"reflect"
	"rentroll/rlib"
	"strconv"
	"strings"
)

// CreatePeopleCSV create people csv temporarily
// write headers, used to load data from roomkey csv
// return file pointer to call program
func CreatePeopleCSV(
	CSVStore string,
	timestamp string,
	peopleCSVStruct *core.PeopleCSV,
) (*os.File, *csv.Writer, bool) {

	var done = false

	// get path of people csv file
	filePrefix := prefixCSVFile["people"]
	fileName := filePrefix + timestamp + ".csv"
	peopleCSVFilePath := path.Join(CSVStore, fileName)

	// try to create file and return with error if occurs any
	peopleCSVFile, err := os.Create(peopleCSVFilePath)
	if err != nil {
		rlib.Ulog("Error <PEOPLE CSV>: %s\n", err.Error())
		return nil, nil, done
	}

	// create csv writer
	peopleCSVWriter := csv.NewWriter(peopleCSVFile)

	// parse headers of peopleCSV using reflect
	peopleCSVHeaders, ok := core.GetStructFields(peopleCSVStruct)
	if !ok {
		rlib.Ulog("Error <PEOPLE CSV>: Unable to get struct fields for peopleCSV\n")
		return nil, nil, done
	}

	peopleCSVWriter.Write(peopleCSVHeaders)
	peopleCSVWriter.Flush()

	done = true

	return peopleCSVFile, peopleCSVWriter, done
}

// ReadPeopleCSVData used to read the data for People csv
// from roomkey csv file while avoiding duplicate data
func ReadPeopleCSVData(
	recordCount *int,
	rowIndex int,
	traceCSVData map[int]int,
	csvRow []string,
	avoidData *[]string,
	suppliedValues map[string]string,
	peopleStruct *core.PeopleCSV,
	tracePeopleNote map[int]string,
	traceDuplicatePeople map[string][]string,
	csvErrors map[int][]string,
	guestData []string,
	guestCSVSupplied bool,
	guestHeaderMap map[string]core.CSVHeader,
	peopleCSVData *[][]string,
	csvHeaderMap map[string]core.CSVHeader,
) {

	// flag duplicate people
	rowName := strings.TrimSpace(csvRow[csvHeaderMap["Guest"].Index])
	name := strings.ToLower(rowName)

	// flag for name of people who has no email or phone
	if name != "" {
		if core.StringInSlice(name, traceDuplicatePeople["name"]) {
			warnPrefix := "W:<" + core.DBTypeMapStrings[core.DBPeople] + ">:"
			// mark it as a warning so customer can validate it
			csvErrors[rowIndex] = append(csvErrors[rowIndex],
				warnPrefix+"There is at least one other person with the name \""+rowName+"\" "+
					"who also has no unique identifiers such as cell phone number or email.",
			)
		} else {
			traceDuplicatePeople["name"] = append(traceDuplicatePeople["name"], name)
		}
	}

	// get csv row data
	csvRowData := GetPeopleCSVRow(
		csvRow, peopleStruct,
		suppliedValues, rowIndex,
		tracePeopleNote,
		guestData, guestCSVSupplied,
		guestHeaderMap, csvHeaderMap,
	)

	*peopleCSVData = append(*peopleCSVData, csvRowData)

	// entry this rowindex with unit value in the map
	*recordCount = *recordCount + 1
	traceCSVData[*recordCount+1] = rowIndex
}

// GetPeopleCSVRow used to create people
// csv row from roomkey csv data
func GetPeopleCSVRow(
	roomkeyRow []string,
	fieldMap *core.PeopleCSV,
	DefaultValues map[string]string,
	rowIndex int,
	tracePeopleNote map[int]string,
	guestData []string,
	guestCSVSupplied bool,
	guestHeaderMap map[string]core.CSVHeader,
	csvHeaderMap map[string]core.CSVHeader,
) []string {

	// ======================================
	// Load people's data from roomkeyrow data
	// ======================================
	reflectedPeopleFieldMap := reflect.ValueOf(fieldMap).Elem()

	// length of PeopleCSV
	pplLength := reflectedPeopleFieldMap.NumField()

	// return data array
	dataMap := make(map[int]string)

	for i := 0; i < pplLength; i++ {
		// get people field
		peopleField := reflectedPeopleFieldMap.Type().Field(i)

		// if peopleField value exist in DefaultValues map
		// then set it first
		suppliedValue, found := DefaultValues[peopleField.Name]
		if found {
			dataMap[i] = strings.TrimSpace(suppliedValue)
		}

		if guestCSVSupplied {

			if len(guestData) > 0 && guestData[guestHeaderMap["GuestName"].Index] != "" {
				if peopleField.Name == "FirstName" {
					dataMap[i] = strings.TrimSpace(guestData[guestHeaderMap["FirstName"].Index])
				}
				if peopleField.Name == "LastName" {
					dataMap[i] = strings.TrimSpace(guestData[guestHeaderMap["LastName"].Index])
				}
				if peopleField.Name == "PrimaryEmail" {
					if core.IsValidEmail(guestData[guestHeaderMap["Email"].Index]) {
						dataMap[i] = strings.TrimSpace(guestData[guestHeaderMap["Email"].Index])
					}
				}
				if peopleField.Name == "CellPhone" {
					dataMap[i] = strings.TrimSpace(guestData[guestHeaderMap["MainPhone"].Index])
				}
				if peopleField.Name == "Address" {
					dataMap[i] = strings.TrimSpace(guestData[guestHeaderMap["Address"].Index])
				}
				if peopleField.Name == "Address2" {
					dataMap[i] = strings.TrimSpace(guestData[guestHeaderMap["Address2"].Index])
				}
				if peopleField.Name == "City" {
					dataMap[i] = strings.TrimSpace(guestData[guestHeaderMap["City"].Index])
				}
				if peopleField.Name == "State" {
					dataMap[i] = strings.TrimSpace(guestData[guestHeaderMap["StateProvince"].Index])
				}
				if peopleField.Name == "PostalCode" {
					dataMap[i] = strings.TrimSpace(guestData[guestHeaderMap["ZipPostalCode"].Index])
				}
				if peopleField.Name == "Country" {
					dataMap[i] = strings.TrimSpace(guestData[guestHeaderMap["Country"].Index])
				}
				if peopleField.Name == "AlternateAddress" {
					dataMap[i] = strings.TrimSpace(guestData[guestHeaderMap["Address2"].Index])
				}
			}
		}
		// =========================================================
		// these conditions have been put here because it's mapping field does not exist
		// =========================================================
		if peopleField.Name == "FirstName" {
			nameSlice := strings.Split(roomkeyRow[csvHeaderMap["Guest"].Index], ",")
			dataMap[i] = strings.TrimSpace(nameSlice[0])
		}
		if peopleField.Name == "LastName" {
			nameSlice := strings.Split(roomkeyRow[csvHeaderMap["Guest"].Index], ",")
			if len(nameSlice) > 1 {
				dataMap[i] = strings.TrimSpace(nameSlice[1])
			} else {
				dataMap[i] = ""
			}
		}

		// Special notes for people to get TCID in future with below value

		// Add description to Notes field of people
		if peopleField.Name == "Notes" {
			des := roomkeyNotesPrefix + strconv.Itoa(rowIndex) + "." + descriptionFieldSep
			des += "Res:" + roomkeyRow[csvHeaderMap["Res"].Index] + "."
			if roomkeyRow[csvHeaderMap["Description"].Index] != "" {
				des += descriptionFieldSep + strings.TrimSpace(roomkeyRow[csvHeaderMap["Description"].Index])
			}
			dataMap[i] = des
			tracePeopleNote[rowIndex] = des
		}

		// get mapping field
		MappedFieldName := reflectedPeopleFieldMap.FieldByName(peopleField.Name).Interface().(string)

		// if has not value then continue
		if header, ok := csvHeaderMap[MappedFieldName]; ok {
			dataMap[i] = strings.TrimSpace(roomkeyRow[header.Index])
		} else {
			continue
		}
	}

	dataArray := []string{}

	for i := 0; i < pplLength; i++ {
		dataArray = append(dataArray, dataMap[i])
	}

	return dataArray
}
