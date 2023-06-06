First show vector exhaustion

In dplyr lets look at rides that cost more than $5 and group them by day of the week then hour. Do people tip more on friday nights? Less on Sunday mornings?

```
Rscript dplyr-vector-exhaustion.R
```
dplyr runs into vector memory exhaustion. The same query with duckplyr does not


```
Rscript duckplyr_can_read.R
```

Let's filter to trips over $50 in the month of december and in november (people are more thankful or giving). Let's do dplyr first.
```
Rscript dplyr-tips.R
```
We do 2 queries so we can show the speed of dplyr once the data has all been loaded into memory.
```
First Query (december)
25.161s
Second Query (november)
6.271s
```


```
Rscript duckplyr_tips.R
First query (december)
3.409s
Second query (november)
3.228
```

Clearly duckplyr is faster. But this is only for fares over $50. Let's see if we can do fares over $2 dollars.
```
Rscript duckplyr_tips_all.R
First query (december)
4.798s
Second query (november)
4.562s
```





Timing results

total amount of trip > 10 and month = 12
tips (by day & hour)

168 groups (7 day per week * 24 hours)

Duckplyr: 4.582 total
dplyr: # (can't even run on my machine)

Duckplyr: 3.237s
dplyr: 4.123s


