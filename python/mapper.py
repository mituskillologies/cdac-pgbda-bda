#!/usr/bin/python3
import sys
for line in sys.stdin:
    words = line.split()
    for word in words:
       print(word, 1)
