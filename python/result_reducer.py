#!/usr/bin/python3
import sys
mx = 0
result = []
for line in sys.stdin:
	name, marks = line.split()
	result.append([name, marks])
	if(float(marks) > mx):
		mx = float(marks)

for row in result:
	if float(row[1]) == mx:
		print(row[0], row[1])
