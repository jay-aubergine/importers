package roomkey

import (
	"importers/core"
	"rentroll/rlib"
	"strconv"
	"strings"
	"time"
)

// getFormattedDate returns rentroll accepted date string
func getFormattedDate(
	dateString string,
) string {

	const shortForm = "02-Jan-2006"
	const layout = "2006-01-02"

	parsedDate, _ := time.Parse(shortForm, dateString)
	return parsedDate.Format(layout)

}

// csvRecordsToSkip function that should check an error
// which contains such a thing that needs to be discard
// such as. already exists, already done. etc. . . .
func csvRecordsToSkip(err error) bool {
	for _, dup := range csvRecordsSkipList {
		if strings.Contains(err.Error(), dup) {
			return true
		}
	}
	return false
}

// TO PARSE LINE, COLUMN, ITEM, ERROR TEXT FROM RCSV ERRORS ONLY
func parseLineAndErrorFromRCSV(rcsvErr error, dbType int) (int, int, string, bool) {
	/*
		This parsing is only works with below pattern
		========================================
		{FunctionName}: line {LineNumber}, column {ColumnNumber}, item {ItemNumber} >>> errorReason
		or
		{FunctionName}: line {LineNumber}, column {ColumnNumber} >>> errorReason
		========================================
		if other pattern supplied for error then it fails
	*/

	errText := rcsvErr.Error()
	// split with separator `:` breaks into [0]{FuncName} and [1]rest of the text
	// split at most 2 substrings only
	s := strings.SplitN(errText, ":", 2)
	// we need only text without {FuncName}
	errText = s[1]
	// split with separator `>>>` breaks into [0] line no, column no, item no string and [1] actual reason for error which we want to show to user
	// split at most 2 substrings only
	s = strings.SplitN(errText, ">>>", 2)

	// parse error reason =================
	// now we only need the exact reason
	errText = strings.TrimSpace(s[1])
	// remove new line broker
	errText = strings.Replace(errText, "\n", "", -1)
	// consider this as Errors so need to prepand <E:>
	errText = "E:<" + core.DBTypeMapStrings[dbType] + ">:" + errText

	// parse line number, column number, item number(if present)=================

	data := strings.Split(s[0], ",")
	lineNoStr := ""
	columnNoStr := "-1"
	itemNoStr := "-1"
	errFormat := ""

	// get line number string
	lineNoStr = strings.TrimSpace(data[0])
	// get column number string
	columnNoStr = strings.TrimSpace(data[1])

	if len(data) == 2 {
		errFormat = "{FunctionName}: line {LineNumber}, column {ColumnNumber} >>> errorReason"
	} else if len(data) == 3 {
		// get item number string
		itemNoStr = strings.TrimSpace(data[2])
		errFormat = "{FunctionName}: line {LineNumber}, column {ColumnNumber}, item {ItemNumber} >>> errorReason"
	}

	// remove `line` text from lineNoStr string
	lineNoStr = strings.Replace(lineNoStr, "line", "", -1)
	// remove space from lineNoStr string
	lineNoStr = strings.TrimSpace(lineNoStr)
	// now it should contain number in string
	lineNo, err := strconv.Atoi(lineNoStr)
	if err != nil {
		// CRITICAL
		rlib.Ulog("INTERNAL ERRORS: RCSV Error is not in format of `%s` for error: %s", errFormat, errText)
		return lineNo, -1, errText, false
	}

	// remove `column` text from columnNoStr string
	columnNoStr = strings.Replace(columnNoStr, "column", "", -1)
	// remove space from columnNoStr string
	columnNoStr = strings.TrimSpace(columnNoStr)
	// now it should contain number in string
	_, err = strconv.Atoi(columnNoStr)
	if err != nil {
		// CRITICAL
		rlib.Ulog("INTERNAL ERRORS: RCSV Error is not in format of `%s` for error: %s", errFormat, errText)
		return lineNo, -1, errText, false
	}

	// remove `item` text from itemNoStr string
	itemNoStr = strings.Replace(itemNoStr, "item", "", -1)
	// remove space from itemNoStr string
	itemNoStr = strings.TrimSpace(itemNoStr)
	// now it should contain number in string
	itemNo, err := strconv.Atoi(itemNoStr)
	if err != nil {
		// CRITICAL
		rlib.Ulog("INTERNAL ERRORS: RCSV Error is not in format of `%s` for error: %s", errFormat, errText)
		return lineNo, itemNo, errText, false
	}

	//return
	return lineNo, itemNo, errText, true
}
