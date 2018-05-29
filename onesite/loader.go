package onesite

import (
	"context"
	"fmt"
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

// loadOneSiteCSV loads the values from the supplied csv file and
// creates rlib.Business records as needed
func loadOneSiteCSV(
	ctx context.Context,
	oneSiteCSV string,
	testMode int,
	userRRValues map[string]string,
	business *rlib.Business,
	currentTime time.Time,
	currentTimeFormat string,
	summaryReport map[int]map[string]int,
) (map[int]string, map[int][]string, bool) {

	internalErrFlag := true
	csvErrors := map[int][]string{}

	// this count used to skip number of rows from the very top of csv
	var skipRowsCount int

	// traceTCIDMap hold TCID for each people to be loaded via people csv
	// with reference of original onesite csv
	traceTCIDMap := map[int]string{}

	// traceUnitMap holds records by which we can trace the unit with row index of csv
	// Unit would be unique in onesite imported csv
	// key: rowIndex of onesite csv, value: Unit value of each row of onesite csv
	traceUnitMap := map[int]string{}

	// ================================================
	// LOAD FIELD MAP AND GET HEADERS, LENGTH OF HEADERS
	// ================================================

	folderPath, err := osext.ExecutableFolder()
	if err != nil {
		rlib.Ulog("INTERNAL ERROR <ONESITE GETTING FOLDERPATH>: %s\n", err.Error())
		return traceUnitMap, csvErrors, internalErrFlag
	}

	// read json file which contains mapping of onesite fields
	mapperFilePath := path.Join(folderPath, "mapper.json")

	var oneSiteFieldMap core.CSVFieldMap
	err = core.GetFieldMapping(&oneSiteFieldMap, mapperFilePath)
	if err != nil {
		rlib.Ulog("INTERNAL ERROR <ONESITE FIELD MAPPING>: %s\n", err.Error())
		return traceUnitMap, csvErrors, internalErrFlag
	}

	// get Headers of csv
	headerFilePath := path.Join(folderPath, "header.json")

	csvHeaderList, err := core.GetCSVHeaders(headerFilePath)
	if err != nil {
		rlib.Ulog("INTERNAL ERROR <ONESITE GETTING CSV HEADERS>: %s\n", err.Error())
		return traceUnitMap, csvErrors, internalErrFlag
	}

	// load csv file and get data from csv
	t := rlib.LoadCSV(oneSiteCSV)

	// iterate over csv data to detect headers first
	for rowIndex := 0; rowIndex < len(t); rowIndex++ {
		for colIndex := 0; colIndex < len(t[rowIndex]); colIndex++ {
			// remove all white spaces and make lower case
			cellTextValue := strings.ToLower(
				core.SpecialCharsReplacer.Replace(t[rowIndex][colIndex]))

			// ********************************
			// MARKET RENT OR MARKET ADDL
			// ********************************
			// if marketRent found then remove marketAddl header
			// and make an entry for "marketrent" in csvColumnFieldMap with -1
			// keep "MarketAddl" mapping to `marketrent` still, anyways `MarketAddl`
			// going to be put in `MarketRate` of Rentroll field
			if cellTextValue == marketRent {
				for i := range csvHeaderList {
					if csvHeaderList[i].HeaderText == "marketaddl" {
						csvHeaderList[i].HeaderText = marketRent
						csvHeaderList[i].Name = "MarketAddl"
					}
				}
			}
			// assign column index in struct if header text match from cell data
			for i := range csvHeaderList {
				if csvHeaderList[i].HeaderText == cellTextValue {
					csvHeaderList[i].Index = colIndex
				}
			}
		}

		// check after row columns parsing that headers are found or not
		headersFound := true
		for i := range csvHeaderList {
			if csvHeaderList[i].Index == -1 && !csvHeaderList[i].IsOptional {
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

	// if headers not found then
	if skipRowsCount == 0 {
		missingHeaders := []string{}
		// make message of missing columns
		for i := range csvHeaderList {
			if csvHeaderList[i].Index == -1 && !csvHeaderList[i].IsOptional {
				missingHeaders = append(missingHeaders, string(csvHeaderList[i].Name))
			}
		}

		headerError := "Required data column(s) missing: "
		headerError += strings.Join(missingHeaders, ", ")

		// ******** special entry ***********
		/*// -1 means there is no data
		internalErrFlag = false*/
		csvErrors[-1] = append(csvErrors[-1], headerError)
		return traceUnitMap, csvErrors, internalErrFlag
	}

	// map for csv headers in onesite csv file to access data fastly
	// by it's header name rather than iterating over slice every time
	// to look for a specific CSVHeader
	var csvHeaderMap = make(map[string]core.CSVHeader)
	for _, header := range csvHeaderList {
		csvHeaderMap[header.Name] = header
	}

	// --------------------------- csv record count ----------------------------
	// <TYPE>CSVRecordCount used to hold records count inserted in csv
	// initialize with 1 because first row contains headers in target generated csv
	// these are POSSIBLE record count that going to be imported
	RentableTypeCSVRecordCount := 0
	CustomAttributeCSVRecordCount := 0
	PeopleCSVRecordCount := 0
	RentableCSVRecordCount := 0
	RentalAgreementCSVRecordCount := 0
	CustomAttrRefRecordCount := 0

	// --------------------------- trace data map ---------------------------- //
	// trace<TYPE>CSVMap used to hold records
	// by which we can traceout which records has been writtern to csv
	// with key of row index of <TARGET_TYPE> CSV, value of original's imported csv rowNumber
	traceRentableTypeCSVMap := map[int][]int{}
	traceCustomAttributeCSVMap := map[int][]int{}
	tracePeopleCSVMap := map[int][]int{}
	traceRentableCSVMap := map[int][]int{}
	traceRentalAgreementCSVMap := map[int][]int{}

	// traceDuplicatePeople holds records with unique string (name, email, phone)
	// with duplicant match at row
	// e.g.; {
	// 	"phone": {"9999999999": [2,4]},
	// 	"name": {"foo, bar": 3},
	// }
	traceDuplicatePeople := map[string][]string{
		"name":  {},
		"phone": {},
	}

	// --------------------------- variables to hold the data to be written------------------
	// <TYPE>CSVData used to hold the data of the <Type>.csv file
	var rentableTypeCSVData [][]string
	var customAttributeCSVData [][]string
	var peopleCSVData [][]string
	var rentableCSVData [][]string
	var rentalAgreementCSVData [][]string

	// --------------------- avoid duplicate data structures -------------------- //
	// avoidDuplicateRentableTypeData used to keep track of rentableTypeData with Style field
	// so that duplicate entries can be avoided while creating rentableType csv file
	avoidDuplicateRentableTypeData := []string{}

	// avoidDuplicateCustomAttributeData is tricky map which holds the
	// duplicate data in slice for each field defined in customAttributeMap
	avoidDuplicateCustomAttributeData := map[string][]string{}
	for k := range customAttributeMap {
		avoidDuplicateCustomAttributeData[k] = []string{}
	}

	// avoidDuplicateUnit is used to keep track of rows with same Unit in onesite csv
	// so that rows with duplicate Unit can be combined.
	avoidDuplicateUnit := []string{}

	// customAttributesRefData holds the data after customAttr insertion
	// to insert custom attribute ref in system for each rentableType
	// so we identify each element in this list with Style Key
	customAttributesRefData := map[string]CARD{}

	// =================================
	// DELETE DATA RELATED TO BUSINESS ID
	// =================================
	// delete business related data before starting to import in database
	_, err = rlib.DeleteBusinessFromDB(ctx, business.BID)
	if err != nil {
		rlib.Ulog("INTERNAL ERROR <DELETE BUSINESS>: %s\n", err.Error())
		return traceUnitMap, csvErrors, internalErrFlag
	}

	bid, err := rlib.InsertBusiness(ctx, business)
	if err != nil {
		rlib.Ulog("INTERNAL ERROR <INSERT BUSINESS>: %s\n", err.Error())
		return traceUnitMap, csvErrors, internalErrFlag
	}
	// set new BID as we have deleted and inserted it again
	// TODO:  remove this step after sman's next push
	// in InsertBusiness it will be set automatically
	business.BID = bid

	// ========================================================
	// WRITE DATA FOR CUSTOM ATTRIBUTE, RENTABLE TYPE, PEOPLE CSV
	// ========================================================

	// get created rentabletype csv and writer pointer
	rentableTypeCSVFile, rentableTypeCSVWriter, ok :=
		CreateRentableTypeCSV(
			TempCSVStore, currentTimeFormat,
			&oneSiteFieldMap.RentableTypeCSV,
		)
	if !ok {
		rlib.Ulog("INTERNAL ERROR <RENTABLE TYPE CSV>: %s\n", err.Error())
		return traceUnitMap, csvErrors, internalErrFlag
	}

	// get created customAttibutes csv and writer pointer
	customAttributeCSVFile, customAttributeCSVWriter, ok :=
		CreateCustomAttibutesCSV(
			TempCSVStore, currentTimeFormat,
			&oneSiteFieldMap.CustomAttributeCSV,
		)
	if !ok {
		rlib.Ulog("INTERNAL ERROR <CUSTOM ATTRIUTE CSV>: %s\n", err.Error())
		return traceUnitMap, csvErrors, internalErrFlag
	}

	// get created people csv and writer pointer
	peopleCSVFile, peopleCSVWriter, ok :=
		CreatePeopleCSV(
			TempCSVStore, currentTimeFormat,
			&oneSiteFieldMap.PeopleCSV,
		)
	if !ok {
		rlib.Ulog("INTERNAL ERROR <PEOPLE CSV>: %s\n", err.Error())
		return traceUnitMap, csvErrors, internalErrFlag
	}

	// once headers are found, then look for the data
	for rowIndex := skipRowsCount; rowIndex <= len(t); rowIndex++ {

		// blank cell values count
		blankCellCount := 0

		// unavailable fields in csv data
		unavailableFields := 0

		for _, header := range csvHeaderList {
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
		if blankCellCount+unavailableFields == len(csvHeaderList) {
			// what IF, only headers are there
			if (rowIndex) == skipRowsCount {
				// ******** special entry ***********
				// -1 means there is no data
				internalErrFlag = false
				csvErrors[-1] = append(csvErrors[-1], "There are no data rows present")
				return traceUnitMap, csvErrors, internalErrFlag
			} /*else {
				// blank row found, can't proceed further
			}*/
			// else break the loop as there are no more data
			break
		}

		traceUnitMap[rowIndex] = t[rowIndex][csvHeaderMap["Unit"].Index]

		// get rentable status and evaluate whether for particular element
		// we can import data or not
		csvRentableStatus := t[rowIndex][csvHeaderMap["UnitLeaseStatus"].Index]
		_, rrUseStatus, _ := IsValidRentableUseStatus(csvRentableStatus)
		csvTypesSet := canWriteCSVStatusMap[rrUseStatus]
		var canReadData bool

		// check first that for this row's status rentableType data can be read
		canReadData = core.IntegerInSlice(core.RENTABLETYPECSV, csvTypesSet)
		if canReadData {
			ReadRentableTypeCSVData(
				&RentableTypeCSVRecordCount,
				rowIndex,
				traceRentableTypeCSVMap,
				t[rowIndex],
				&rentableTypeCSVData,
				&avoidDuplicateRentableTypeData,
				currentTime,
				userRRValues,
				&oneSiteFieldMap.RentableTypeCSV,
				customAttributesRefData,
				csvHeaderMap,
				business,
			)
		}

		// check first that for this row's status custom attributes data can be read
		canReadData = core.IntegerInSlice(core.CUSTOMATTRIUTESCSV, csvTypesSet)
		if canReadData {
			ReadCustomAttributeCSVData(
				&CustomAttributeCSVRecordCount,
				rowIndex,
				traceCustomAttributeCSVMap,
				t[rowIndex],
				&customAttributeCSVData,
				avoidDuplicateCustomAttributeData,
				userRRValues,
				csvHeaderMap,
			)
		}

		// check first that for this row's status people data can be read
		canReadData = core.IntegerInSlice(core.PEOPLECSV, csvTypesSet)
		if canReadData {
			traceTCIDMap[rowIndex] = ""
			ReadPeopleCSVData(
				&PeopleCSVRecordCount,
				rowIndex,
				tracePeopleCSVMap,
				t[rowIndex],
				&peopleCSVData,
				traceDuplicatePeople,
				currentTimeFormat,
				userRRValues,
				&oneSiteFieldMap.PeopleCSV,
				csvErrors,
				csvHeaderMap,
			)
		}
	}

	for _, data := range rentableTypeCSVData {
		rentableTypeCSVWriter.Write(data)
		rentableTypeCSVWriter.Flush()
	}
	for _, data := range customAttributeCSVData {
		customAttributeCSVWriter.Write(data)
		customAttributeCSVWriter.Flush()
	}
	for _, data := range peopleCSVData {
		peopleCSVWriter.Write(data)
		peopleCSVWriter.Flush()
	}

	// Close all files as we are done here with writing data
	rentableTypeCSVFile.Close()
	customAttributeCSVFile.Close()
	peopleCSVFile.Close()

	// =======================
	// NESTED UTILITY FUNCTIONS
	// =======================

	// getTraceDataMap from string name
	getTraceDataMap := func(traceDataMapName string) map[int][]int {
		switch traceDataMapName {
		case "traceCustomAttributeCSVMap":
			return traceCustomAttributeCSVMap
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

	// getIndexAndUnit used to get index and unit value from trace<TYPE>CSVMap map
	getIndexAndUnit := func(traceDataMap map[int][]int, index int, itemNo int) (int, string) {
		var onesiteIndex int
		var unit string
		if itemNo == -1 {
			itemNo = 0
		} else {
			itemNo--
		}
		if onesiteIndexSlice, ok := traceDataMap[index]; ok {
			onesiteIndex = onesiteIndexSlice[itemNo]
			if unit, ok := traceUnitMap[onesiteIndex]; ok {
				return onesiteIndex, unit
			}
			return onesiteIndex, unit
		}
		return onesiteIndex, unit
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
				lineNo, itemNo, reason, ok := parseLineAndErrorFromRCSV(err, dbType)
				if !ok {
					// INTERNAL ERROR - RETURN FALSE
					return false
				}
				// get tracedatamap
				traceDataMap := getTraceDataMap(traceDataMapName)
				// now get the original row index of imported onesite csv and Unit value
				onesiteIndex, _ := getIndexAndUnit(traceDataMap, lineNo, itemNo)
				// generate new error
				csvErrors[onesiteIndex] = append(csvErrors[onesiteIndex], reason)
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
				lineNo, itemNo, _, ok := parseLineAndErrorFromRCSV(err, dbType)
				if !ok {
					// INTERNAL ERROR - RETURN FALSE
					return false
				}
				// get tracedatamap
				traceDataMap := getTraceDataMap(traceDataMapName)
				// now get the original row index of imported onesite csv and Unit value
				onesiteIndex, _ := getIndexAndUnit(traceDataMap, lineNo, itemNo)
				// load csvRow from dataMap to get email
				csvRow := t[onesiteIndex]
				pEmail := csvRow[csvHeaderMap["Email"].Index]
				// get tcid from email
				t, tErr := rlib.GetTransactantByPhoneOrEmail(ctx, business.BID, pEmail)
				if tErr != nil {
					// unable to get TCID
					reason := "E:<" + core.DBTypeMapStrings[core.DBPeople] + ">:Unable to get people information" + err.Error()
					csvErrors[onesiteIndex] = append(csvErrors[onesiteIndex], reason)
				} else if t.TCID == 0 {
					// unable to get TCID
					reason := "E:<" + core.DBTypeMapStrings[core.DBPeople] + ">:Unable to get people information"
					csvErrors[onesiteIndex] = append(csvErrors[onesiteIndex], reason)
				} else {
					// if duplicate people found
					rlib.Ulog("DUPLICATE RECORD ERROR <%s>: %s", fname, err.Error())
					// map it in tcid map
					traceTCIDMap[onesiteIndex] = tcidPrefix + strconv.FormatInt(t.TCID, 10)
				}
			} else {
				lineNo, itemNo, reason, ok := parseLineAndErrorFromRCSV(err, dbType)
				if !ok {
					// INTERNAL ERROR - RETURN FALSE
					return false
				}
				// get tracedatamap
				traceDataMap := getTraceDataMap(traceDataMapName)
				// now get the original row index of imported onesite csv and Unit value
				onesiteIndex, _ := getIndexAndUnit(traceDataMap, lineNo, itemNo)
				// generate new error
				csvErrors[onesiteIndex] = append(csvErrors[onesiteIndex], reason)
			}

			// *****************************************************************
			// AS WE DON'T HAVE MAPPING OF PHONENUMBER TO CELLPHONE
			// WE JUST AVOID THIS CHECK, BUT KEEP THIS IN CASE MAPPING
			// OF PHONENUMBER CHANGED TO CELLPHONE
			// PLACE IT AFTER DUPLICATE EMAIL CHECK
			// *****************************************************************
			/*
				else if strings.Contains(errText, dupTransactantWithCellPhone) {
					lineNo, itemNo, _, ok := parseLineAndErrorFromRCSV(err, dbType)
					if !ok {
						// INTERNAL ERROR - RETURN FALSE
						return false
					}
					// get tracedatamap
					traceDataMap := getTraceDataMap(traceDataMapName)
					// now get the original row index of imported onesite csv and Unit value
					onesiteIndex, unit := getIndexAndUnit(traceDataMap, lineNo, itemNo)
					// load csvRow from dataMap to get email
					csvRow := t[onesiteIndex]
					pCellNo := csvRow[csvHeaderMap["PhoneNumber"].Index]
					// get tcid from cellphonenumber
					t, tErr := rlib.GetTransactantByPhoneOrEmail(ctx, business.BID, pCellNo)
					if tErr != nil {
						// unable to get TCID
						reason := "E:<" + core.DBTypeMapStrings[core.DBPeople] + ">:Unable to get people information" + err.Error()
						csvErrors[onesiteIndex] = append(csvErrors[onesiteIndex], reason)
					} else if t.TCID == 0{
						// unable to get TCID
						reason := "E:<" + core.DBTypeMapStrings[core.DBPeople] + ">:Unable to get people information"
						csvErrors[onesiteIndex] = append(csvErrors[onesiteIndex], reason)
					} else {
						// if duplicate people found
						rlib.Ulog("DUPLICATE RECORD ERROR <%s>: %s", fname, err.Error())
						// map it in tcid map
						traceTCIDMap[onesiteIndex] = tcidPrefix + strconv.FormatInt(t.TCID, 10)
					}
				}
			*/
			// *****************************************************

		}
		// return with success
		return true
	}

	// =========================================
	// LOAD CUSTOM ATTRIBUTE & RENTABLE TYPE CSV
	// =========================================
	var h = []csvLoadHandler{
		{Fname: customAttributeCSVFile.Name(), Handler: rcsv.LoadCustomAttributesCSV, TraceDataMap: "traceCustomAttributeCSVMap", DBType: core.DBCustomAttr},
		{Fname: rentableTypeCSVFile.Name(), Handler: rcsv.LoadRentableTypesCSV, TraceDataMap: "traceRentableTypeCSVMap", DBType: core.DBRentableType},
	}

	for i := 0; i < len(h); i++ {
		if len(h[i].Fname) > 0 {
			if !rrDoLoad(ctx, h[i].Fname, h[i].Handler, h[i].TraceDataMap, h[i].DBType) {
				// INTERNAL ERROR
				return traceUnitMap, csvErrors, internalErrFlag
			}
		}
	}

	// =====================================
	// INSERT CUSTOM ATTRIBUTE REF MANUALLY
	// AFTER CUSTOM ATTRIB AND RENTABLE TYPE
	// LOADED SUCCESSFULLY
	// =====================================

	// always sort keys
	var customAttributesRefDataKeys []string
	for k := range customAttributesRefData {
		customAttributesRefDataKeys = append(customAttributesRefDataKeys, k)
	}
	sort.Strings(customAttributesRefDataKeys)

	for _, key := range customAttributesRefDataKeys {
		errPrefix := "E:<" + core.DBTypeMapStrings[core.DBCustomAttrRef] + ">:"
		// find rentableType
		refData := customAttributesRefData[key]
		rt, err := rlib.GetRentableTypeByStyle(ctx, refData.Style, refData.BID)
		if err != nil {
			rlib.Ulog("ERROR <CUSTOMREF INSERTION>: %s", err.Error())
			csvErrors[refData.RowIndex] = append(csvErrors[refData.RowIndex], errPrefix+"Unable to insert custom attribute")
			continue
		}

		// for all custom attribute defined in custom_attrib.go
		// find custom attribute ID
		for _, customAttributeConfig := range customAttributeMap {
			t, _ := strconv.ParseInt(customAttributeConfig["ValueType"], 10, 64)
			n := customAttributeConfig["Name"]
			v := strconv.Itoa(int(refData.SqFt))
			u := customAttributeConfig["Units"]
			ca, err := rlib.GetCustomAttributeByVals(ctx, t, n, v, u)
			if err != nil {
				rlib.Ulog("ERROR <CUSTOMREF INSERTION>: %s", "CUSTOM ATTRIBUTE NOT FOUND IN DB")
				csvErrors[refData.RowIndex] = append(csvErrors[refData.RowIndex], errPrefix+"Unable to insert custom attribute")
				continue
			}
			// if resource not found then continue
			if ca.CID == 0 {
				rlib.Ulog("ERROR <CUSTOMREF INSERTION>: %s", "CUSTOM ATTRIBUTE NOT FOUND IN DB")
				csvErrors[refData.RowIndex] = append(csvErrors[refData.RowIndex], errPrefix+"Unable to insert custom attribute")
				continue
			}

			// count possible values
			CustomAttrRefRecordCount++

			// insert custom attribute ref in system
			var a rlib.CustomAttributeRef
			a.ElementType = rlib.ELEMRENTABLETYPE
			a.BID = business.BID
			a.ID = rt.RTID
			a.CID = ca.CID

			// check that record already exists, if yes then just continue
			// without accounting it as an error
			ref, err := rlib.GetCustomAttributeRef(ctx, a.ElementType, a.ID, a.CID)
			if err != nil {
				rlib.Ulog("ERROR <CUSTOMREF INSERTION>: %s", err.Error())
				continue
			} else {
				if ref.ElementType == a.ElementType && ref.CID == a.CID && ref.ID == a.ID {
					unit, _ := traceUnitMap[refData.RowIndex]
					errText := fmt.Sprintf(
						"This reference already exists. No changes were made. at row \"%d\" with unit \"%s\"",
						refData.RowIndex, unit)
					rlib.Ulog("ERROR <CUSTOMREF INSERTION>: %s", errText)
					continue
				}
			}

			_, err = rlib.InsertCustomAttributeRef(ctx, &a)
			if err != nil {
				rlib.Ulog("ERROR <CUSTOMREF INSERTION>: %s", err.Error())
				csvErrors[refData.RowIndex] = append(csvErrors[refData.RowIndex], errPrefix+"Unable to insert custom attribute")
				continue
			}
		}
	}

	// ================
	// LOAD PEOPLE CSV
	// ================
	h = []csvLoadHandler{
		{Fname: peopleCSVFile.Name(), Handler: rcsv.LoadPeopleCSV, TraceDataMap: "tracePeopleCSVMap", DBType: core.DBPeople},
	}

	for i := 0; i < len(h); i++ {
		if len(h[i].Fname) > 0 {
			if !rrPeopleDoLoad(ctx, h[i].Fname, h[i].Handler, h[i].TraceDataMap, h[i].DBType) {
				// INTERNAL ERROR
				return traceUnitMap, csvErrors, internalErrFlag
			}
		}
	}

	// ========================================================
	// GET TCID FOR EACH ROW FROM PEOPLE CSV AND UPDATE TCID MAP
	// ========================================================

	// here we pass "0" as a parameter just to satisfy the function
	// later on in next line we neglect the last character i.e. "0" (that we passed to function)
	noteString := getPeopleNoteString(0, currentTimeFormat)
	tcidMap, _ := rlib.GetTCIDByNote(ctx, "%"+noteString[:len(noteString)-1]+"%")

	for tcid, note := range tcidMap {
		note_temp := strings.SplitN(note, "$", 2)
		note_temp = strings.SplitN(note_temp[1], "$", 2)
		onesiteIndex, _ := strconv.Atoi(note_temp[1])

		traceTCIDMap[onesiteIndex-1] = tcidPrefix + strconv.Itoa(int(tcid))
	}

	// ==============================================================
	// AFTER POSSIBLE TCID FOUND, WRITE RENTABLE & RENTAL AGREEMENT CSV
	// ==============================================================

	// get created people csv and writer pointer
	rentableCSVFile, rentableCSVWriter, ok :=
		CreateRentableCSV(
			TempCSVStore, currentTimeFormat,
			&oneSiteFieldMap.RentableCSV,
		)
	if !ok {
		rlib.Ulog("INTERNAL ERROR <RENTABLE CSV>: %s\n", err.Error())
		return traceUnitMap, csvErrors, internalErrFlag
	}

	// get created rental agreement csv and writer pointer
	rentalAgreementCSVFile, rentalAgreementCSVWriter, ok :=
		CreateRentalAgreementCSV(
			TempCSVStore, currentTimeFormat,
			&oneSiteFieldMap.RentalAgreementCSV,
		)
	if !ok {
		rlib.Ulog("INTERNAL ERROR <RENTAL AGREEMENT CSV>: %s\n", err.Error())
		return traceUnitMap, csvErrors, internalErrFlag
	}

	// traceRentableUnitMap := map[int]string{}
	traceRentableUnitMap := map[string]int{}

	// always sort keys to iterate over csv rows in proper manner (from top to bottom)
	var csvRowKeys []int
	for k := range traceUnitMap {
		csvRowKeys = append(csvRowKeys, k)
	}
	sort.Ints(csvRowKeys)

	for _, rowIndex := range csvRowKeys {
		// get rentable status and evaluate whether for particular element
		// we can import data or not
		csvRentableStatus := t[rowIndex][csvHeaderMap["UnitLeaseStatus"].Index]
		_, rrUseStatus, _ := IsValidRentableUseStatus(csvRentableStatus)
		csvTypesSet := canWriteCSVStatusMap[rrUseStatus]
		var canReadData bool

		// check first that for this row's status rentable data can be read
		canReadData = core.IntegerInSlice(core.RENTABLECSV, csvTypesSet)
		if canReadData {
			ReadRentableCSVData(
				&RentableCSVRecordCount,
				rowIndex,
				traceRentableCSVMap,
				t[rowIndex],
				&rentableCSVData,
				&avoidDuplicateUnit,
				currentTime,
				userRRValues,
				&oneSiteFieldMap.RentableCSV,
				traceTCIDMap,
				csvErrors,
				rrUseStatus,
				csvHeaderMap,
				traceRentableUnitMap,
			)
		}

		// check first that for this row's status rental aggrement data can be read
		canReadData = core.IntegerInSlice(core.RENTALAGREEMENTCSV, csvTypesSet)
		if canReadData {
			ReadRentalAgreementCSVData(
				&RentalAgreementCSVRecordCount,
				rowIndex,
				traceRentalAgreementCSVMap,
				t[rowIndex],
				&rentalAgreementCSVData,
				currentTime,
				userRRValues,
				&oneSiteFieldMap.RentalAgreementCSV,
				traceTCIDMap,
				csvErrors,
				csvHeaderMap,
			)
		}
	}

	for _, data := range rentableCSVData {
		rentableCSVWriter.Write(data)
		rentableCSVWriter.Flush()
	}
	rentableCSVFile.Close()

	for _, data := range rentalAgreementCSVData {
		rentalAgreementCSVWriter.Write(data)
		rentalAgreementCSVWriter.Flush()
	}
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
				return traceUnitMap, csvErrors, internalErrFlag
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
	summaryReport[core.DBCustomAttr]["possible"] = CustomAttributeCSVRecordCount
	summaryReport[core.DBCustomAttrRef]["possible"] = CustomAttrRefRecordCount
	summaryReport[core.DBPeople]["possible"] = PeopleCSVRecordCount

	// printMap(csvErrors)

	internalErrFlag = false
	return traceUnitMap, csvErrors, internalErrFlag
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

func CSVHandler(
	ctx context.Context,
	csvPath string,
	testMode int,
	userRRValues map[string]string,
	business *rlib.Business,
	debugMode int,
) (string, bool, bool) {

	// return report, internal error flag, done (csv loaded or not)

	// csv loaded successfully flag
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
		core.DBCustomAttr:      {"imported": 0, "possible": 0, "issues": 0},
		core.DBRentableType:    {"imported": 0, "possible": 0, "issues": 0},
		core.DBCustomAttrRef:   {"imported": 0, "possible": 0, "issues": 0},
		core.DBPeople:          {"imported": 0, "possible": 0, "issues": 0},
		core.DBRentable:        {"imported": 0, "possible": 0, "issues": 0},
		core.DBRentalAgreement: {"imported": 0, "possible": 0, "issues": 0},
	}

	// ====== Call onesite loader =====
	unitMap, csvErrs, internalErr := loadOneSiteCSV(ctx,
		csvPath, testMode, userRRValues,
		business, currentTime, currentTimeFormat,
		summaryReportCount)

	// if internal error then just return from here, nothing to do
	if internalErr {
		return csvReport, internalErr, csvLoaded
	}

	// check if there any errors from onesite loader
	if len(csvErrs) > 0 {
		csvReport, csvLoaded = errorReporting(ctx, business, csvErrs, unitMap, summaryReportCount, csvPath, debugMode, currentTime)

		// if not testmode then only do rollback
		if testMode != 1 {
			rollBackImportOperation(currentTimeFormat)
		}

		return csvReport, internalErr, csvLoaded
	}

	// ===== 4. Generate Report =====
	csvReport = successReport(ctx, business, summaryReportCount, csvPath, debugMode, currentTime)

	// ===== 5. Return =====
	return csvReport, internalErr, csvLoaded
}
