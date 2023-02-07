#!/usr/bin/python3
import sys
post = 0
date = 0
for line in sys.stdin:
	words = line.split(",")
	if words[9].isdigit() and post < int(words[9]):
		post = int(words[9])
		date = words[2]

print("Funniest Post on date",date,"Total hahas:",post);
