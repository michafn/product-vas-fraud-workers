FROM golang:1.18-alpine

RUN apk add --no-cache git make

WORKDIR /opt/workdir
COPY . ./
RUN make
CMD [ "./bin/fraudSync" ]
