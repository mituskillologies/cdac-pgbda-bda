#!/usr/bin/python3
import sys
total = []
for line in sys.stdin:
	word, num = line.split()
	total.append(int(num))

print("Average shares:", sum(total)/len(total))
