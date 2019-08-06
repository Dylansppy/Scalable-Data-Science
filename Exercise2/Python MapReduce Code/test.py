# Test mapper.py and reducer.py locally first

# very basic test
Dylans-MacBook-Pro:~ dylan$ chmod +x ~/Documents/UC/S2_DATA420_Scalable/Exercises/Exercise2/mapper.py

Dylans-MacBook-Pro:~ dylan$ echo "foo foo quux labs foo bar quux" | \
                            ~/Documents/UC/S2_DATA420_Scalable/Exercises/Exercise2/mapper.py
foo     1
foo     1
quux    1
labs    1
foo     1
bar     1
quux    1

Dylans-MacBook-Pro:~ dylan$ chmod +x ~/Documents/UC/S2_DATA420_Scalable/Exercises/Exercise2/reducer.py

Dylans-MacBook-Pro:~ dylan$ echo "foo foo quux labs foo bar quux" | \
                            Documents/UC/S2_DATA420_Scalable/Exercises/Exercise2/mapper.py | sort -k1,1 | \
                            Documents/UC/S2_DATA420_Scalable/Exercises/Exercise2/reducer.py
bar     1
foo     3
labs    1
quux    2

# using one of the ebooks as example input
# (see below on where to get the ebooks)
Dylans-MacBook-Pro:~ dylan$ cat Documents/UC/S2_DATA420_Scalable/Exercises/Exercise2/gutenberg/pg20417.txt | \
                                Documents/UC/S2_DATA420_Scalable/Exercises/Exercise2/mapper.py
 The     1
 Project 1
 Gutenberg       1
 EBook   1
 of      1
[...]

Dylans-MacBook-Pro:~ dylan$ cat Documents/UC/S2_DATA420_Scalable/Exercises/Exercise2/gutenberg/pg20417.txt | \
                                Documents/UC/S2_DATA420_Scalable/Exercises/Exercise2/mapper.py | sort -k1,1 | \
                                Documents/UC/S2_DATA420_Scalable/Exercises/Exercise2/reducer.py


