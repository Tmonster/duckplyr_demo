# Install dplyr and other basics
Rscript -e "install.packages('pak', tidyverse', 'duckdb')"

# Install duckplyr
pak::pak("duckdblabs/duckplyr")

# unzip taxi-data-2019.zip
unzip taxi-data-2019.zip