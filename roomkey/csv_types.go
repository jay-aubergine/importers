package roomkey

import (
	"importers/core"
	"strings"
)

// by which index, it will decide tyep of row
var rowTypeDetectionCSVIndex = map[string]int{
	"page":        0,
	"description": 2,
}

// loadRoomKeyCSVRow used to load data from slice
// into CSVRow struct and return that struct
func loadRoomKeyCSVRow(csvHeaderMap map[string]core.CSVHeader,
	data []string, isPageZero bool) (bool, []string) {

	skipRow := false

	csvRow := make([]string, len(data))

	for _, header := range csvHeaderMap {
		if header.Index == -1 && !header.IsOptional {
			skipRow = true
			return skipRow, csvRow
		}
	}

	for _, header := range csvHeaderMap {
		if isPageZero && (header.HeaderText == "room" ||
			header.HeaderText == "roomtype" ||
			header.HeaderText == "rate" ||
			header.HeaderText == "ratename" ||
			header.HeaderText == "groupcorporatename") {

			csvRow[header.Index] = data[header.Index+3]

		} else {
			csvRow[header.Index] = data[header.Index]
		}
	}

	blankCellCount := 0
	for _, cellData := range csvRow {
		if cellData == "" {
			blankCellCount++
		}
	}

	// if blank data has not been passed then only need to return true
	if blankCellCount == len(csvRow) {
		skipRow = true
	}
	return skipRow, csvRow
}

// check that row is headerline
func isRoomKeyHeaderLine(rowHeaders []string, isPageZero bool,
	headerList []core.CSVHeader) (bool, map[string]core.CSVHeader) {

	csvHeaderList := make([]core.CSVHeader, 0)
	for _, header := range headerList {
		csvHeaderList = append(csvHeaderList, header)
	}

	for colIndex := 0; colIndex < len(rowHeaders); colIndex++ {
		// remove all white spaces and make lower case
		cellTextValue := strings.ToLower(
			core.SpecialCharsReplacer.Replace(rowHeaders[colIndex]))

		// assign column index in struct if header text match from cell data
		for i := range csvHeaderList {

			// here we need to set index explicitly
			// because there will be no header with name "description" in csv
			if csvHeaderList[i].HeaderText == "description" {
				csvHeaderList[i].Index = 2
				continue
			}

			if csvHeaderList[i].HeaderText == cellTextValue {
				if csvHeaderList[i].HeaderText == "datein" {
					csvHeaderList[i].Index = colIndex + 1
					continue
				}
				// we take colIndex-3 if it is data of page 0
				// because there are 3 blank columns
				if isPageZero {
					if csvHeaderList[i].HeaderText == "room" ||
						csvHeaderList[i].HeaderText == "roomtype" ||
						csvHeaderList[i].HeaderText == "rate" ||
						csvHeaderList[i].HeaderText == "ratename" ||
						csvHeaderList[i].HeaderText == "groupcorporatename" {

						csvHeaderList[i].Index = colIndex - 3
						continue
					}

				}
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

	csvHeaderMap := map[string]core.CSVHeader{}
	for _, header := range csvHeaderList {
		csvHeaderMap[header.Name] = header
	}
	return headersFound, csvHeaderMap
}

// isRoomKeyPageRow check row is used for new page records
func isRoomKeyPageRow(data []string) bool {
	// if first column is not empty then it is
	return strings.TrimSpace(data[rowTypeDetectionCSVIndex["page"]]) != ""
}

func isRoomKeyDescriptionRow(data []string) bool {
	// if third column is not empty then it is
	return strings.TrimSpace(data[rowTypeDetectionCSVIndex["description"]]) != ""
}
