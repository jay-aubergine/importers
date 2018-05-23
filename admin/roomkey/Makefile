clean:
	go clean
	rm -f roomkey.log report1.txt report2.txt business.txt
	rm -rf temp_CSVs

db:
	${GOPATH}/src/rentroll/tmp/rentroll/rrnewdb
	${GOPATH}/src/rentroll/tmp/rentroll/rrloadcsv -noauth -b ./business.csv >./business.txt 2>&1

build:
	go build

report: 
	# ./roomkey -bud RKEY -csv ../../csvfiles_temp/roomkey.csv -guestinfo ../../csvfiles_temp/guest.csv -noauth -testmode=1
	./roomkey -bud RKEY -csv ../../csvfiles_temp/roomkey.csv -noauth -testmode=1 > report1.txt
	./roomkey -bud RKEY -csv ../../csvfiles_temp/roomkey.csv -guestinfo ../../csvfiles_temp/guest.csv -noauth -testmode=1 > report2.txt
all: clean db build report