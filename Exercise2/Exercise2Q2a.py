# pipling the file containing 8 lines of 'hello world' as  input
(base) Dylans-MBP:~ dylan$ cd Documents/UC/S2_DATA420_Scalable/Exercises/Exercise2

(base) Dylans-MBP:Exercise2 dylan$ cat ./Q2a_test.txt | ./mapper.py

(base) Dylans-MBP:Exercise2 dylan$ cat ./Q2a_test.txt | ./mapper.py | sort -k1,1 | ./reducer.py