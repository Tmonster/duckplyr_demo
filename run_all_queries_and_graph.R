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
  q1_duckplyr, q2_duckplyr, q3_duckplyr, q4_duckplyr,
  q1_dplyr, q2_dplyr, q3_dplyr, q4_dplyr
)

timings <- data.frame(
  query = c("query 1", "query 2", "query 3", "query 4"),
  time = all_timings,
  system = c("duckplyr", "duckplyr", "duckplyr", "duckplyr", "dplyr", "dplyr", "dplyr", "dplyr")
)

bargraph <- ggplot(timings, aes(x = query, y = time, fill = system)) +
  geom_col(position = "dodge") +
  theme(text = element_text(size = 20))

# save the data
write.csv(timings, "timings.csv", row.names = FALSE)

# plot
dpi <- 96
ggsave(filename = "timings.jpg", plot = bargraph, width = 900 / dpi, height = 500 / dpi, dpi = dpi)

save(list = c("q1_dplyr", "q2_dplyr", "q3_dplyr", "q4_dplyr"), file = "dplyr.rda")
