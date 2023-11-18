FROM golang:latest

RUN go env -w GO111MODULE=off
ADD main.go /prj4/
ADD hostsfile-testcase1.txt /prj4/
ADD hostsfile-testcase2.txt /prj4/
WORKDIR /prj4/ 
RUN go build -o main


ENTRYPOINT ["/prj4/main"]