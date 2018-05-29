package onesite

import (
	"encoding/csv"
	"importers/core"
	"os"
	"path"
	"reflect"
	"rentroll/rlib"
	"strings"
)

// CreatePeopleCSV create people csv temporarily
// write headers, used to load data from onesite csv
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

// ReadPeopleCSVData used to read the data for People csv file
// from onesite csv while avoiding duplicate data
func ReadPeopleCSVData(
	recordCount *int,
	rowIndex int,
	traceCSVData map[int][]int,
	csvRow []string,
	peopleCSVData *[][]string,
	traceDuplicatePeople map[string][]string,
	currentTimeFormat string,
	suppliedValues map[string]string,
	peopleStruct *core.PeopleCSV,
	csvErrors map[int][]string,
	csvHeaderMap map[string]core.CSVHeader,
) {

	// flag duplicate people
	rowName := strings.TrimSpace(csvRow[csvHeaderMap["Name"].Index])
	name := strings.ToLower(rowName)

	email := ""
	phone := ""

	if csvHeaderMap["Email"].Index != -1 {
		email = strings.ToLower(strings.TrimSpace(csvRow[csvHeaderMap["Email"].Index]))
	}
	if csvHeaderMap["PhoneNumber"].Index != -1 {
		phone = strings.TrimSpace(csvRow[csvHeaderMap["PhoneNumber"].Index])
	}

	// flag for name of people who has no email or phone
	if name != "" && email == "" && phone == "" {
		if core.StringInSlice(name, traceDuplicatePeople["name"]) {
			warnPrefix := "W:<" + core.DBTypeMapStrings[core.DBPeople] + ">:"
			// mark it as a warning so customer can validate it
			csvErrors[rowIndex+1] = append(csvErrors[rowIndex+1],
				warnPrefix+"There is at least one other person with the name \""+rowName+"\" "+
					"who also has no unique identifiers such as cell phone number or email.",
			)
		} else {
			traceDuplicatePeople["name"] = append(traceDuplicatePeople["name"], name)
		}
	}

	// flag for phone with same person name only
	if phone != "" {
		if core.StringInSlice(phone, traceDuplicatePeople["phone"]) &&
			core.StringInSlice(name, traceDuplicatePeople["name"]) {
			warnPrefix := "W:<" + core.DBTypeMapStrings[core.DBPeople] + ">:"
			// mark it as a warning so customer can validate it
			csvErrors[rowIndex+1] = append(csvErrors[rowIndex+1],
				warnPrefix+"There is at least one other person with the same name \""+name+"\" and work phone \""+phone+"\""+
					" and no other unique identifiers such as cell phone or email",
			)
		} else {
			traceDuplicatePeople["phone"] = append(traceDuplicatePeople["phone"], phone)
		}
	}

	// get csv row data
	csvRowData := GetPeopleCSVRow(
		csvRow, peopleStruct,
		currentTimeFormat,
		suppliedValues, rowIndex,
		csvHeaderMap,
	)

	*peopleCSVData = append(*peopleCSVData, csvRowData)

	*recordCount = *recordCount + 1

	// need to map on next row index of temp csv as first row is header line
	// and recordCount initialized with 0 value
	traceCSVData[*recordCount+1] = append(traceCSVData[*recordCount+1], rowIndex+1)
}

// GetPeopleCSVRow used to create people
// csv row from onesite csv data
func GetPeopleCSVRow(
	oneSiteRow []string,
	fieldMap *core.PeopleCSV,
	timestamp string,
	DefaultValues map[string]string,
	rowIndex int,
	csvHeaderMap map[string]core.CSVHeader,
) []string {

	// ======================================
	// Load people's data from onesiterow data
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
			dataMap[i] = suppliedValue
		}

		// =========================================================
		// this condition has been put here because it's mapping field does not exist
		// =========================================================
		if peopleField.Name == "LastName" {
			nameSlice := strings.Split(oneSiteRow[csvHeaderMap["Name"].Index], ",")
			dataMap[i] = strings.TrimSpace(nameSlice[0])
		}
		if peopleField.Name == "FirstName" {
			nameSlice := strings.Split(oneSiteRow[csvHeaderMap["Name"].Index], ",")
			if len(nameSlice) > 1 {
				dataMap[i] = strings.TrimSpace(nameSlice[1])
			} else {
				dataMap[i] = ""
			}
		}
		// Special notes for people to get TCID in future with below value
		if peopleField.Name == "Notes" {
			dataMap[i] = getPeopleNoteString(rowIndex+1, timestamp)
		}

		// get mapping field
		MappedFieldName := reflectedPeopleFieldMap.FieldByName(peopleField.Name).Interface().(string)

		if (MappedFieldName == "Email" || MappedFieldName == "PhoneNumber") && csvHeaderMap[MappedFieldName].Index == -1 {
			continue
		}

		// get field by mapping field name and then value
		if header, ok := csvHeaderMap[MappedFieldName]; ok {
			dataMap[i] = oneSiteRow[header.Index]
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
