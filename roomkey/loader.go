package roomkey

import (
	"context"
	"errors"
	"importers/core"
	"os"
	"path"
	"rentroll/rcsv"
	"rentroll/rlib"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kardianos/osext"
)

func loadRoomKeyCSV(
	ctx context.Context,
	roomKeyCSV string,
	guestInfo map[string][]string,
	guestHeaderMap map[string]core.CSVHeader,
	guestCSVSupplied bool,
	testMode int,
	userRRValues map[string]string,
	business *rlib.Business,
	currentTime time.Time,
	currentTimeFormat string,
	summaryReport map[int]map[string]int,
) (map[int][]string, bool) {

	// returns csvError list, csv loaded?

	// returned csv errors should be in format
	// {
	// 	"rowIndex": ["E:errors",....., "W:warnings",....]
	// }
	// E stands for Error string, W stands for Warning string
	// UnitName can be accessible via traceUnitMap

	// =========================
	// DATA STRUCTURES AND VARS
	// =========================

	internalErrFlag := true
	csvErrors := map[int][]string{}

	// this holds the records for each row index
	csvRowDataMap := map[int][]string{}

	// ================================================
	// LOAD FIELD MAP AND GET HEADERS, LENGTH OF HEADERS
	// ================================================

	folderPath, err := osext.ExecutableFolder()
	if err != nil {
		rlib.Ulog("INTERNAL ERROR <ROOMKEY GETTING FOLDERPATH>: %s\n", err.Error())
		return csvErrors, internalErrFlag
	}

	// read json file which contains mapping of onesite fields
	mapperFilePath := path.Join(folderPath, "mapper.json")

	var RoomKeyFieldMap core.CSVFieldMap
	err = core.GetFieldMapping(&RoomKeyFieldMap, mapperFilePath)
	if err != nil {
		rlib.Ulog("INTERNAL ERROR <ROOMKEY FIELD MAPPING>: %s\n", err.Error())
		return csvErrors, internalErrFlag
	}

	// get Headers of csv
	headerFilePath := path.Join(folderPath, "roomkeyHeader.json")

	csvHeaderList, err := core.GetCSVHeaders(headerFilePath)
	if err != nil {
		rlib.Ulog("INTERNAL ERROR <ROOMKEY GETTING CSV HEADERS>: %s\n", err.Error())
		return csvErrors, internalErrFlag
	}

	// load csv file and get data from csv
	t := rlib.LoadCSV(roomKeyCSV)

	// this will be helpful while we have "description" type of row
	// so that we can put it in currentDataRowIndex's csvRow
	currentDataRowIndex := 0

	headersFirstOccurenceFound := false

	// map for csv headers in onesite csv file to access data fastly
	// by it's header name rather than iterating over slice every time
	// to look for a specific CSVHeader
	csvHeaderMap := map[string]core.CSVHeader{}

	isPageZero := true
	for rowIndex := 1; rowIndex <= len(t); rowIndex++ {

		// if it is header line then skip it
		if ok, headerMap := isRoomKeyHeaderLine(t[rowIndex-1], isPageZero, csvHeaderList); ok {
			csvHeaderMap = headerMap
			headersFirstOccurenceFound = true

			continue
		}

		// if first time headers are not detected then do continue
		if !headersFirstOccurenceFound {
			continue
		}

		// check it is page row
		if isRoomKeyPageRow(t[rowIndex-1]) {
			isPageZero = false
			continue
		}

		// check it is description row
		if isRoomKeyDescriptionRow(t[rowIndex-1]) {
			csvRowDataMap[currentDataRowIndex][2] += descriptionFieldSep + strings.TrimSpace(t[rowIndex-1][2])
			// csvRowDataMap[currentDataRowIndex].Description += descriptionFieldSep + strings.TrimSpace(t[rowIndex-1][rowTypeDetectionCSVIndex["description"]])
			continue
		}

		skipRow, csvRow := loadRoomKeyCSVRow(csvHeaderList, csvHeaderMap, t[rowIndex-1], isPageZero)

		if skipRow {
			// in case blank row detected
			continue
		}

		// map this row as currentDataRowIndex and also hold a reference in datamap
		csvRowDataMap[rowIndex] = csvRow
		currentDataRowIndex = rowIndex

	}

	// if csvRowDataMap is empty, that means data could not be parsed from csv
	if len(csvRowDataMap) == 0 {
		internalErrFlag = false
		csvErrors[-1] = append(csvErrors[-1], "There are no data rows present")
		return csvErrors, internalErrFlag
	}

	// =================================
	// DELETE DATA RELATED TO BUSINESS ID
	// =================================
	// detele business related data before starting to import in database
	_, err = rlib.DeleteBusinessFromDB(ctx, business.BID)
	if err != nil {
		rlib.Ulog("INTERNAL ERROR <DELETE BUSINESS>: %s\n", err.Error())
		return csvErrors, internalErrFlag
	}

	_, err = rlib.InsertBusiness(ctx, business)
	if err != nil {
		rlib.Ulog("INTERNAL ERROR <INSERT BUSINESS>: %s\n", err.Error())
		return csvErrors, internalErrFlag
	}

	// ========================================================
	// WRITE DATA FOR RENTABLE TYPE, PEOPLE CSV
	// ========================================================

	// get created rentabletype csv and writer pointer
	rentableTypeCSVFile, rentableTypeCSVWriter, ok :=
		CreateRentableTypeCSV(
			TempCSVStore, currentTimeFormat,
			&RoomKeyFieldMap.RentableTypeCSV,
		)
	if !ok {
		rlib.Ulog("INTERNAL ERROR <RENTABLE TYPE CSV>\n")
		return csvErrors, internalErrFlag
	}

	// get created people csv and writer pointer
	peopleCSVFile, peopleCSVWriter, ok :=
		CreatePeopleCSV(
			TempCSVStore, currentTimeFormat,
			&RoomKeyFieldMap.PeopleCSV,
		)
	if !ok {
		rlib.Ulog("INTERNAL ERROR <PEOPLE CSV>: %s\n", err.Error())
		return csvErrors, internalErrFlag
	}

	// To store the keys in slice in sorted order
	// always sort keys to iterate over csv rows in proper manner (from top to bottom)
	var csvRowDataMapKeys []int
	for k := range csvRowDataMap {
		csvRowDataMapKeys = append(csvRowDataMapKeys, k)
	}
	sort.Ints(csvRowDataMapKeys)

	// --------------------- avoid duplicate data structures -------------------- //
	// avoidDuplicateRentableTypeData used to keep track of rentableTypeData with Style field
	// so that duplicate entries can be avoided while creating rentableType csv file
	avoidDuplicateRentableTypeData := []string{}
	avoidDuplicatePeopleData := []string{}

	// --------------------------- csv record count ----------------------------
	// <TYPE>CSVRecordCount used to hold records count inserted in csv
	// initialize with 1 because first row contains headers in target generated csv
	// these are POSSIBLE record count that going to be imported
	RentableTypeCSVRecordCount := 0
	RentableCSVRecordCount := 0
	PeopleCSVRecordCount := 0
	RentalAgreementCSVRecordCount := 0

	// --------------------------- trace csv records map ----------------------------
	// trace<TYPE>CSVMap used to hold records
	// by which we can traceout which records has been writtern to csv
	// with key of row index of <TARGET_TYPE> CSV, value of original's imported csv rowNumber
	traceRentableTypeCSVMap := map[int]int{}
	tracePeopleCSVMap := map[int]int{}
	traceRentableCSVMap := map[int]int{}
	traceRentalAgreementCSVMap := map[int]int{}

	// traceTCIDMap hold TCID for each people to be loaded via people csv
	// with reference of original roomkey csv
	traceTCIDMap := map[int]string{}

	// tracePeopleNote holds people note with reference of original roomkey csv
	tracePeopleNote := map[int]string{}

	// peopleCollisions holds count of people with same name
	peopleCollisions := map[string]int{}

	// traceDuplicatePeople holds records with unique string (name, email, phone)
	// with duplicant match at row
	// e.g.; {
	// 	"name": {"foo, bar": 3},
	// }
	traceDuplicatePeople := map[string][]string{
		"name": {},
	}

	// --------------------------- variables to hold the data to be written------------------
	// <TYPE>CSVData used to hold the data of the <Type>.csv file
	var rentableTypeCSVData [][]string
	var peopleCSVData [][]string
	var rentableCSVData [][]string
	var rentalAgreementCSVData [][]string

	// Iterating over cleaned csv data
	for _, rowIndex := range csvRowDataMapKeys {

		csvRow := csvRowDataMap[rowIndex]

		// Read data for rentabletype csv
		ReadRentableTypeCSVData(
			&RentableTypeCSVRecordCount,
			rowIndex,
			traceRentableTypeCSVMap,
			csvRow,
			&avoidDuplicateRentableTypeData,
			currentTime,
			userRRValues,
			&RoomKeyFieldMap.RentableTypeCSV,
			business,
			&rentableTypeCSVData,
			csvHeaderMap,
		)

		guestdata := []string{}
		if guestCSVSupplied {
			if data, ok := guestInfo[csvRow[csvHeaderMap["Guest"].Index]]; ok {
				guestdata = data
			}
		}

		traceTCIDMap[rowIndex] = ""
		tracePeopleNote[rowIndex] = csvRow[csvHeaderMap["Description"].Index]

		peopleCollisions[csvRow[csvHeaderMap["Guest"].Index]]++
		if peopleCollisions[csvRow[csvHeaderMap["Guest"].Index]] > 1 {
			guestdata = []string{}
		}

		// Read data for people csv
		ReadPeopleCSVData(
			&PeopleCSVRecordCount,
			rowIndex,
			tracePeopleCSVMap,
			csvRow,
			&avoidDuplicatePeopleData,
			userRRValues,
			&RoomKeyFieldMap.PeopleCSV,
			tracePeopleNote,
			traceDuplicatePeople,
			csvErrors,
			guestdata,
			guestCSVSupplied,
			guestHeaderMap,
			&peopleCSVData,
			csvHeaderMap,
		)

	}

	for _, data := range rentableTypeCSVData {
		rentableTypeCSVWriter.Write(data)
		rentableTypeCSVWriter.Flush()
	}

	for _, data := range peopleCSVData {
		peopleCSVWriter.Write(data)
		peopleCSVWriter.Flush()
	}
	// Close all files as we are done here with writing data
	rentableTypeCSVFile.Close()
	peopleCSVFile.Close()

	// =======================
	// NESTED UTILITY FUNCTIONS
	// =======================

	// getTraceDataMap from string name
	getTraceDataMap := func(traceDataMapName string) map[int]int {
		switch traceDataMapName {
		case "traceRentableTypeCSVMap":
			return traceRentableTypeCSVMap
		case "tracePeopleCSVMap":
			return tracePeopleCSVMap
		case "traceRentableCSVMap":
			return traceRentableCSVMap
		case "traceRentalAgreementCSVMap":
			return traceRentalAgreementCSVMap
		default:
			return nil
		}
	}

	// getRoomKeyIndex used to get index and unit value from trace<TYPE>CSVMap map
	getRoomKeyIndex := func(traceDataMap map[int]int, index int) int {
		var roomKeyIndex int
		if roomKeyIndex, ok := traceDataMap[index]; ok {
			return roomKeyIndex
		}
		return roomKeyIndex
	}

	// rrDoLoad is a nested function
	// used to load data from csv with help of rcsv loaders
	rrDoLoad := func(ctx context.Context, fname string, handler func(context.Context, string) []error, traceDataMapName string, dbType int) bool {
		Errs := handler(ctx, fname)

		for _, err := range Errs {
			// skip warnings about already existing records
			// if it's not kind of to skip then process it and count in error report
			errText := err.Error()

			if !csvRecordsToSkip(err) {
				lineNo, _, reason, ok := parseLineAndErrorFromRCSV(err, dbType)
				if !ok {
					// INTERNAL ERROR - RETURN FALSE
					return false
				}
				// get tracedatamap
				traceDataMap := getTraceDataMap(traceDataMapName)
				// now get the original row index of imported onesite csv and Unit value
				roomKeyIndex := getRoomKeyIndex(traceDataMap, lineNo)
				// generate new error
				csvErrors[roomKeyIndex] = append(csvErrors[roomKeyIndex], reason)
			} else {
				rlib.Ulog("DUPLICATE RECORD ERROR <%s>: %s\n", fname, errText)
			}
		}
		// return with success
		return true
	}

	// *****************************************************
	// rrPeopleDoLoad (SPECIAL METHOD TO LOAD PEOPLE)
	// *****************************************************
	rrPeopleDoLoad := func(ctx context.Context, fname string, handler func(context.Context, string) []error, traceDataMapName string, dbType int) bool {
		Errs := handler(ctx, fname)

		for _, err := range Errs {
			// handling for duplicant transactant
			if strings.Contains(err.Error(), dupTransactantWithPrimaryEmail) {
				lineNo, _, _, ok := parseLineAndErrorFromRCSV(err, dbType)
				if !ok {
					// INTERNAL ERROR - RETURN FALSE
					return false
				}
				// get tracedatamap
				traceDataMap := getTraceDataMap(traceDataMapName)
				// now get the original row index of imported onesite csv and Unit value
				roomkeyIndex := getRoomKeyIndex(traceDataMap, lineNo)

				if _, ok := csvRowDataMap[roomkeyIndex]; !ok {
					continue
				}

				// load csvRow from dataMap troomkeyIndexo get email
				csvRow := csvRowDataMap[roomkeyIndex]

				pEmail := ""
				if guestCSVSupplied {
					if data, ok := guestInfo[csvRow[csvHeaderMap["Guest"].Index]]; ok {
						pEmail = data[guestHeaderMap["Email"].Index]
					}
				}

				// get tcid from email
				t, tErr := rlib.GetTransactantByPhoneOrEmail(ctx, business.BID, pEmail)
				if tErr != nil {
					// t = rlib.GetTransactantByName(business.BID, csvRow.Guest)
					reason := "E:<" + core.DBTypeMapStrings[core.DBPeople] + ">:Unable to get people information" + err.Error()
					csvErrors[roomkeyIndex] = append(csvErrors[roomkeyIndex], reason)
				} else if t.TCID == 0 {
					// t = rlib.GetTransactantByName(business.BID, csvRow.Guest)
					reason := "E:<" + core.DBTypeMapStrings[core.DBPeople] + ">:Unable to get people information"
					csvErrors[roomkeyIndex] = append(csvErrors[roomkeyIndex], reason)
				} else {
					// if duplicate people found
					rlib.Ulog("DUPLICATE RECORD ERROR <%s>: %s", fname, err.Error())
					// map it in tcid map
					traceTCIDMap[roomkeyIndex] = tcidPrefix + strconv.FormatInt(t.TCID, 10)
				}
			} else if strings.Contains(err.Error(), dupTransactantWithCellPhone) {
				lineNo, _, _, ok := parseLineAndErrorFromRCSV(err, dbType)
				if !ok {
					// INTERNAL ERROR - RETURN FALSE
					return false
				}
				// get tracedatamap
				traceDataMap := getTraceDataMap(traceDataMapName)
				// now get the original row index of imported onesite csv and Unit value
				roomkeyIndex := getRoomKeyIndex(traceDataMap, lineNo)

				if _, ok := csvRowDataMap[roomkeyIndex]; !ok {
					continue
				}
				// load csvRow from dataMap to get email
				csvRow := csvRowDataMap[roomkeyIndex]
				// pCellNo := csvRow.PhoneNumber
				pCellNo := ""
				if guestCSVSupplied {
					if data, ok := guestInfo[csvRow[csvHeaderMap["Guest"].Index]]; ok {
						pCellNo = data[guestHeaderMap["MainPhone"].Index]
					}
				}

				// get tcid from cellphonenumber
				t, tErr := rlib.GetTransactantByPhoneOrEmail(ctx, business.BID, pCellNo)
				if tErr != nil {
					// unable to get TCID
					reason := "E:<" + core.DBTypeMapStrings[core.DBPeople] + ">:Unable to get people information" + err.Error()
					csvErrors[roomkeyIndex] = append(csvErrors[roomkeyIndex], reason)
				} else if t.TCID == 0 {
					// unable to get TCID
					reason := "E:<" + core.DBTypeMapStrings[core.DBPeople] + ">:Unable to get people information"
					csvErrors[roomkeyIndex] = append(csvErrors[roomkeyIndex], reason)
				} else {
					// if duplicate people found
					rlib.Ulog("DUPLICATE RECORD ERROR <%s>: %s", fname, err.Error())
					// map it in tcid map
					traceTCIDMap[roomkeyIndex] = tcidPrefix + strconv.FormatInt(t.TCID, 10)
				}
			} else {
				lineNo, _, reason, ok := parseLineAndErrorFromRCSV(err, dbType)
				if !ok {
					// INTERNAL ERROR - RETURN FALSE
					return false
				}
				// get tracedatamap
				traceDataMap := getTraceDataMap(traceDataMapName)
				// now get the original row index of imported onesite csv and Unit value
				roomkeyIndex := getRoomKeyIndex(traceDataMap, lineNo)
				// generate new error
				csvErrors[roomkeyIndex] = append(csvErrors[roomkeyIndex], reason)
			}

			// *****************************************************

		}
		// return with success
		return true
	}

	// ======================
	// LOAD RENTABLE TYPE CSV
	// ======================
	var h = []csvLoadHandler{
		{
			Fname: rentableTypeCSVFile.Name(), Handler: rcsv.LoadRentableTypesCSV,
			TraceDataMap: "traceRentableTypeCSVMap", DBType: core.DBRentableType,
		},
	}

	for i := 0; i < len(h); i++ {
		if len(h[i].Fname) > 0 {
			if !rrDoLoad(ctx, h[i].Fname, h[i].Handler, h[i].TraceDataMap, h[i].DBType) {
				// INTERNAL ERROR
				rlib.Ulog("INTERNAL ERROR <RENTABLE TYPE CSV>\n")
				return csvErrors, internalErrFlag
			}
		}
	}

	// ================
	// LOAD PEOPLE CSV
	// ================
	h = []csvLoadHandler{
		{
			Fname: peopleCSVFile.Name(), Handler: rcsv.LoadPeopleCSV,
			TraceDataMap: "tracePeopleCSVMap", DBType: core.DBPeople,
		},
	}

	for i := 0; i < len(h); i++ {
		if len(h[i].Fname) > 0 {
			if !rrPeopleDoLoad(ctx, h[i].Fname, h[i].Handler, h[i].TraceDataMap, h[i].DBType) {
				// INTERNAL ERROR
				return csvErrors, internalErrFlag
			}
		}
	}

	// ========================================================
	// GET TCID FOR EACH ROW FROM PEOPLE CSV AND UPDATE TCID MAP
	// ========================================================

	for roomkeyIndex := range traceTCIDMap {
		tcid, _ := rlib.GetTCIDByNote(ctx, tracePeopleNote[roomkeyIndex])
		// for duplicant case, it won't be found so need check here
		if tcid != 0 {
			traceTCIDMap[roomkeyIndex] = tcidPrefix + strconv.Itoa(int(tcid))
		}
	}

	// ==============================================================
	// AFTER POSSIBLE TCID FOUND, WRITE RENTABLE & RENTAL AGREEMENT CSV
	// ==============================================================

	// get created people csv and writer pointer
	rentableCSVFile, rentableCSVWriter, ok :=
		CreateRentableCSV(
			TempCSVStore, currentTimeFormat,
			&RoomKeyFieldMap.RentableCSV,
		)
	if !ok {
		rlib.Ulog("INTERNAL ERROR <RENTABLE CSV>: %s\n", err.Error())
		return csvErrors, internalErrFlag
	}

	// get created rental agreement csv and writer pointer
	rentalAgreementCSVFile, rentalAgreementCSVWriter, ok :=
		CreateRentalAgreementCSV(
			TempCSVStore, currentTimeFormat,
			&RoomKeyFieldMap.RentalAgreementCSV,
		)
	if !ok {
		rlib.Ulog("INTERNAL ERROR <RENTAL AGREEMENT CSV>: %s\n", err.Error())
		return csvErrors, internalErrFlag
	}

	// iteration over csv row data structure and write data to csv
	for _, rowIndex := range csvRowDataMapKeys {

		// load csvRow from dataMap
		csvRow := csvRowDataMap[rowIndex]

		// Read data for Rentable csv
		ReadRentableCSVData(
			&RentableCSVRecordCount,
			rowIndex,
			traceRentableCSVMap,
			csvRow,
			currentTime,
			userRRValues,
			&RoomKeyFieldMap.RentableCSV,
			traceTCIDMap,
			csvErrors,
			&rentableCSVData,
			csvHeaderMap,
		)

		// Read data for Rentable csv
		ReadRentalAgreementCSVData(
			&RentalAgreementCSVRecordCount,
			rowIndex,
			traceRentalAgreementCSVMap,
			csvRow,
			currentTime,
			userRRValues,
			&RoomKeyFieldMap.RentalAgreementCSV,
			traceTCIDMap,
			csvErrors,
			&rentalAgreementCSVData,
			csvHeaderMap,
		)
	}

	for _, data := range rentableCSVData {
		rentableCSVWriter.Write(data)
		rentableCSVWriter.Flush()
	}

	for _, data := range rentalAgreementCSVData {
		rentalAgreementCSVWriter.Write(data)
		rentalAgreementCSVWriter.Flush()
	}

	// Close all files as we are done here with writing data
	rentableCSVFile.Close()
	rentalAgreementCSVFile.Close()

	// =====================================
	// LOAD RENTABLE & RENTAL AGREEMENT CSV
	// =====================================
	h = []csvLoadHandler{
		{Fname: rentableCSVFile.Name(), Handler: rcsv.LoadRentablesCSV, TraceDataMap: "traceRentableCSVMap", DBType: core.DBRentable},
		{Fname: rentalAgreementCSVFile.Name(), Handler: rcsv.LoadRentalAgreementCSV, TraceDataMap: "traceRentalAgreementCSVMap", DBType: core.DBRentalAgreement},
	}

	for i := 0; i < len(h); i++ {
		if len(h[i].Fname) > 0 {
			if !rrDoLoad(ctx, h[i].Fname, h[i].Handler, h[i].TraceDataMap, h[i].DBType) {
				// INTERNAL ERROR
				return csvErrors, internalErrFlag
			}
		}
	}

	// ============================
	// CLEAR THE TEMPORARY CSV FILES
	// ============================
	// testmode is not enabled then only remove temp files
	if testMode != 1 {
		clearSplittedTempCSVFiles(currentTimeFormat)
	}

	// ===============================
	// EVALUATE SUMMARY REPORT COUNT
	// ===============================

	// count possible values
	summaryReport[core.DBRentable]["possible"] = RentableCSVRecordCount
	summaryReport[core.DBRentalAgreement]["possible"] = RentalAgreementCSVRecordCount
	summaryReport[core.DBRentableType]["possible"] = RentableTypeCSVRecordCount
	summaryReport[core.DBPeople]["possible"] = PeopleCSVRecordCount

	internalErrFlag = false
	// RETURN
	return csvErrors, internalErrFlag

}

func loadGuestInfoCSV(
	guestInfoCSV string,
) (map[string][]string, map[string]core.CSVHeader, error) {

	// store all guest info in guestInfoMap
	guestInfoMap := make(map[string][]string)

	// map for csv headers in roomkey csv file to access data fastly
	// by it's header name rather than iterating over slice every time
	// to look for a specific CSVHeader
	var guestHeaderMap = make(map[string]core.CSVHeader)

	folderPath, err := osext.ExecutableFolder()
	if err != nil {
		rlib.Ulog("INTERNAL ERROR <ROOMKEY GETTING FOLDERPATH>: %s\n", err.Error())
		return guestInfoMap, guestHeaderMap, err
	}

	guestHeaderFilePath := path.Join(folderPath, "guestHeader.json")

	guestHeaderList, err := core.GetCSVHeaders(guestHeaderFilePath)
	if err != nil {
		rlib.Ulog("INTERNAL ERROR <ROOMKEY GETTING GUEST CSV HEADERS>: %s\n", err.Error())
		return guestInfoMap, guestHeaderMap, err
	}

	// csvHeadersIndex := getGuestCSVHeadersIndexMap()

	skipRowsCount := 0

	// load csv file and get data from csv
	t := rlib.LoadCSV(guestInfoCSV)

	// detect how many rows we need to skip first
	for rowIndex := 0; rowIndex < len(t); rowIndex++ {
		for colIndex := 0; colIndex < len(t[rowIndex]); colIndex++ {
			// remove all white spaces and make lower case
			cellTextValue := strings.ToLower(
				core.SpecialCharsReplacer.Replace(t[rowIndex][colIndex]))
			// if header is exist in map then overwrite it position
			for i := range guestHeaderList {
				if guestHeaderList[i].HeaderText == cellTextValue {
					guestHeaderList[i].Index = colIndex
				}
			}
		}
		// check after row columns parsing that headers are found or not
		headersFound := true
		for i := range guestHeaderList {
			if guestHeaderList[i].Index == -1 && !guestHeaderList[i].IsOptional {
				headersFound = false
				break
			}
		}

		if headersFound {
			// update rowIndex by 1 because we're going to break here
			rowIndex++
			skipRowsCount = rowIndex
			break
		}
	}

	// if skipRowsCount is still 0 that means data could not be parsed from csv
	if skipRowsCount == 0 {
		missingHeaders := []string{}
		// make message of missing columns
		for i := range guestHeaderList {
			if guestHeaderList[i].Index == -1 && !guestHeaderList[i].IsOptional {
				missingHeaders = append(missingHeaders, string(guestHeaderList[i].Name))
			}
		}

		headerError := "(Guest Data CSV) Required data column(s) missing: "
		headerError += strings.Join(missingHeaders, ", ")

		err := errors.New(headerError)
		return guestInfoMap, guestHeaderMap, err
	}

	// set guestHeaderMap if all headers are found and everything is proper
	for _, header := range guestHeaderList {
		guestHeaderMap[header.Name] = header
	}

	// if skipRowsCount found get next row and proceed on rest of the rows with loop
	for rowIndex := skipRowsCount; rowIndex < len(t); rowIndex++ {
		// if column order has been validated then only perform
		// data validation on value, type

		// blank cell values count
		blankCellCount := 0

		// unavailable fields in csv data
		unavailableFields := 0

		for _, header := range guestHeaderList {
			if header.Index == -1 { // if not available
				unavailableFields++
			} else { // if available
				if t[rowIndex][header.Index] == "" { // if data is blank
					blankCellCount++
				}
			}
		}

		// look for blank data in original csv data
		// if blank data found in required columns in a row then break
		// the current loop and avoid to import data further
		if blankCellCount+unavailableFields == len(guestHeaderList) {
			// what IF, only headers are there
			if (rowIndex) == skipRowsCount {
				return guestInfoMap, guestHeaderMap, err
			} /*else {
				// blank row found, can't proceed further
			}*/
			// else break the loop as there are no more data
			break
		}

		guestName := t[rowIndex][guestHeaderMap["GuestName"].Index]
		guestInfoMap[guestName] = t[rowIndex]
	}

	return guestInfoMap, guestHeaderMap, nil
}

// rollBackImportOperation func used to clear out the things
// that created by program temporarily while loading onesite data
//  and if any error occurs
func rollBackImportOperation(timestamp string) {
	clearSplittedTempCSVFiles(timestamp)
}

// clearSplittedTempCSVFiles func used only to clear
// temporarily csv files created by program
func clearSplittedTempCSVFiles(timestamp string) {
	for _, filePrefix := range prefixCSVFile {
		fileName := filePrefix + timestamp + ".csv"
		filePath := path.Join(TempCSVStore, fileName)
		os.Remove(filePath)
	}
}

// CSVHandler is main function to handle user uploaded
// csv and extract information
func CSVHandler(
	ctx context.Context,
	csvPath string,
	GuestInfoCSV string,
	testMode int,
	userRRValues map[string]string,
	business *rlib.Business,
	debugMode int,
) (string, bool, bool) {

	// init values
	csvLoaded := true

	// report text
	csvReport := ""

	// get current timestamp used for creating csv files unique way
	currentTime := time.Now()

	// RFC3339Nano is const format defined in time package
	// <FORMAT> = <SAMPLE>
	// RFC3339Nano = "2006-01-02T15:04:05.999999999Z07:00"
	// it is helpful while creating unique files
	currentTimeFormat := currentTime.Format(time.RFC3339Nano)

	// summaryReportCount contains each type csv as a key
	// with count of total imported, possible, issues in csv data
	summaryReportCount := map[int]map[string]int{
		core.DBRentableType:    {"imported": 0, "possible": 0, "issues": 0},
		core.DBPeople:          {"imported": 0, "possible": 0, "issues": 0},
		core.DBRentable:        {"imported": 0, "possible": 0, "issues": 0},
		core.DBRentalAgreement: {"imported": 0, "possible": 0, "issues": 0},
	}

	// --------------------------------------------------------------------------------------------------------- //

	// here we manually take header index as per the csv format
	// this might be changed in future if column or its order changes in original csv
	// guestHeaderMap := getGuestHeaders()
	var guestHeaderMap map[string]core.CSVHeader

	var guestInfo map[string][]string
	var guestCSVError error

	guestCSVSupplied := false

	// ---------------------- call guestinfocsv loader ----------------------------------------
	// only call if it has been passed then
	if GuestInfoCSV != "" {
		guestCSVSupplied = true
		guestInfo, guestHeaderMap, guestCSVError = loadGuestInfoCSV(GuestInfoCSV)
		if guestCSVError != nil {
			csvReport = "\n\n" + guestCSVError.Error()
			return csvReport, false, false
		}
	}

	// ---------------------- call roomkey loader ----------------------------------------
	csvErrs, internalErr := loadRoomKeyCSV(ctx,
		csvPath, guestInfo, guestHeaderMap, guestCSVSupplied, testMode, userRRValues,
		business, currentTime, currentTimeFormat,
		summaryReportCount)

	// if internal error then just return from here, nothing to do
	if internalErr {
		return csvReport, internalErr, csvLoaded
	}

	// check if there any errors from onesite loader
	if len(csvErrs) > 0 {
		csvReport, csvLoaded = errorReporting(ctx, business, csvErrs, summaryReportCount, csvPath, GuestInfoCSV, debugMode, currentTime)

		// if not testmode then only do rollback
		if testMode != 1 {
			rollBackImportOperation(currentTimeFormat)
		}

		return csvReport, internalErr, csvLoaded
	}

	// ===== 4. Geneate Report =====
	csvReport = successReport(ctx, business, summaryReportCount, csvPath, GuestInfoCSV, debugMode, currentTime)

	// ===== 5. Return =====
	return csvReport, internalErr, csvLoaded

}
