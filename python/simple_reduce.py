import sys
current_word = None
current_count = 0
word = None
for line in sys.stdin:
    word, count = line.split()
    
    if current_word == word:
        current_count += int(count)
    else:
        if current_word:
            print(current_word, current_count)
        current_count = int(count)
        current_word = word

if current_word == word:
    print(current_word, current_count)

