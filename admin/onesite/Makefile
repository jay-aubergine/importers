clean:
	go clean
	rm -f onesite.log report.txt business.txt
	rm -rf temp_CSVs

db:
	${GOPATH}/src/rentroll/tmp/rentroll/rrnewdb
	${GOPATH}/src/rentroll/tmp/rentroll/rrloadcsv -noauth -b ./business.csv >./business.txt 2>&1

build:
	go build

report: 
	./onesite -bud ISO -csv ../../csvfiles_temp/onesite.csv -noauth -testmode=1 > report.txt

secure:
	@rm -f config.json confdev.json confprod.json

all: clean db build report secure