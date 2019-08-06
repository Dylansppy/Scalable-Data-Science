#Q1
result = 0
for i in range(3, 1000):
    if i % 3 == 0 or i % 5  == 0:
        result += i
print(result) #Answer 233168

#Q5
i = 20
not_found = True
while not_found:
    result = 0
    for j in range(1, 21):
        if i % j != 0:
            result += 1
    if result == 0:
        not_found = False
    else:
        i += 20
print(i) #Answer 232792560


#Q10
non_prime = set()
result = 0
for i in range(2, 2000001):
    if i not in non_prime:
        result += i
        non_prime.update(range(i*i, 2000001, i))
print(result) #Answer 142913828922
