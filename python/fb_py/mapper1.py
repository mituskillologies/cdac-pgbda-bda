#!/usr/bin/python3
import sys
for line in sys.stdin:
	words = line.split()
	if words[4] == 'true':
		print(words[1])
