# RUN ALL QUERIES DUCKPLYR, DPLYR and plot timings
options(conflicts.policy = list(warn = FALSE))
library(tidyverse)

if (file.exists("dplyr.rda")) {
  load("dplyr.rda")
} else {
  source("dplyr/run_all_queries.R")

  # remove the data so duckplyr can reload it lazily.
  rm(taxi_data_2019)
  rm(zone_map)
}

source("duckplyr/run_all_queries.R")

all_timings <- c(
  q2_duckplyr, q1_duckplyr, q3_duckplyr, q4_duckplyr,
  q2_dplyr, q1_dplyr, q3_dplyr, q4_dplyr
)

timings <- data.frame(
  query = rep(forcats::fct_inorder(c("Generous crowd", "Party people", "Hot trail", "Cheapskates")), 2),
  time = as.numeric(all_timings),
  system = c("duckplyr", "duckplyr", "duckplyr", "duckplyr", "dplyr", "dplyr", "dplyr", "dplyr")
)

bargraph <- ggplot(timings, aes(x = query, y = time, fill = system)) +
  geom_col(position = "dodge") +
  labs(
    x = "Query",
    y = "Time (seconds)",
    fill = "System",
    title = "Querying NYC taxi data from 2019"
  ) +
  theme(text = element_text(size = 20))

bargraph

speedups <-
  timings |>
  pivot_wider(names_from = system, values_from = time) |>
  mutate(speedup = dplyr / duckplyr)

scattergraph <- ggplot(speedups, aes(x = query, y = speedup)) +
  geom_point() +
  labs(
    x = "Query",
    y = "Speedup",
    title = "Querying NYC taxi data from 2019"
  ) +
  scale_y_log10() +
  theme(text = element_text(size = 20))

scattergraph

# save the data
write.csv(timings, "timings.csv", row.names = FALSE)

# plot
dpi <- 96
ggsave(filename = "timings.pdf", plot = bargraph, width = 900 / dpi, height = 500 / dpi, dpi = dpi)
ggsave(filename = "scattergraph.pdf", plot = scattergraph, width = 900 / dpi, height = 500 / dpi, dpi = dpi)

save(list = c("q1_dplyr", "q2_dplyr", "q3_dplyr", "q4_dplyr"), file = "dplyr.rda")
