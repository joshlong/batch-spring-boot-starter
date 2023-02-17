#!/usr/bin/env bash

$(dirname $0)/gradlew format && git commit -am polish && git push