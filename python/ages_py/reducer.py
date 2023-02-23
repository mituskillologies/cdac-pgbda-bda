#!/usr/bin/python3
import sys
nums = []
for line in sys.stdin:
	word, num = line.split()
	nums.append(int(num))

print('Average:', sum(nums)/len(nums))
