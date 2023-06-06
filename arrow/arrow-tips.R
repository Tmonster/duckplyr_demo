library(tidyverse)
library(arrow)

ds <- open_dataset("/Users/tomebergen/taxi-data-2019/", partitioning = c("month"))

result_1 <- system.time(ds |> filter(total_amount > 2) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = wday(pickup_datetime), hr=hour(pickup_datetime)) |>
  filter(month==12) |>
  # This collect is needed for arrow
  collect() |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
    .by = c(dn, hr)
  ) |>
  arrange(dn, hr) |> 
  print())

print("RESULT 1")
print(result_1)


result_2 <- system.time(ds |> filter(total_amount > 2) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = wday(pickup_datetime), hr=hour(pickup_datetime)) |>
  filter(month==11) |>
  # This collect is needed for arrow
  collect() |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
    .by = c(dn, hr)
  ) |>
  arrange(dn, hr) |> 
  print())

print("RESULT 2")
print(result_2)
