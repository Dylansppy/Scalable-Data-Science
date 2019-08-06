#Q1
result = 0
for (i in 3:999) if (i %% 3 == 0 | i %% 5  == 0) {
  result <- result + i
} 
print(result) #Answer 233168

#Q5
i = 20
not_found <- TRUE
while (not_found == TRUE) { 
  result = 0
  for (j in 1:20) if (i %% j != 0) {
    result <- result + 1
  }
  if (result == 0) {
    not_found = False
  } else {
    i <- i + 20
  }
}
print(i) #Answer 232792560


#Q10
find_prime <- function(n) {
  if (n == 2) return(n)
  result <- 2:n
  i <- 1
  prime <- 2
  while (prime^2 <= n) {
    result <- result[result == prime | result %% prime != 0]
    i <- i + 1 
    prime <- result[i]
  }
  return(result)
}
a = find_prime(2000000)
answer <- sum(a)
print(answer) #Answer 142913828922
