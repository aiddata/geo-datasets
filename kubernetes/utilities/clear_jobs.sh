#!/bin/bash

# takes one argument, number of seconds ago before which to delete jobs

# timestamp before which we delete jobs matching dataset-run-*
# in unix time, of course
AGO="$((`date +%s` - $1))"

# the awk program that parses each line of kubectl command output and decides whether or not to issue a kubectl delete command
handle_lines(){
	# use gawk   set var   sep   pattern match    create date cmd           get cmd output     compare to $AGO     execute kubectl delete command
	/bin/gawk -v ago=$AGO -F' ' '/^dataset-run-/ {cmd="date -d " $2 " +%s"; cmd | getline var; if (int(var) < ago) system("kubectl delete job "$1)}'
}

# go formatting template for kubectl command below
GET_TEMPLATE='{{range .items}}{{.metadata.name}} {{.metadata.creationTimestamp}}{{"\n"}}{{end}}'

# get jobs using template, pass list into handle_lines function above
kubectl get jobs -o go-template --template "$GET_TEMPLATE" | handle_lines
