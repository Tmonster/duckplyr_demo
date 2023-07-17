# Install dplyr and other basics
Rscript -e "install.packages('pak', tidyverse', 'duckdb')"

# Install duckplyr
Rscript -e "pak::pak('duckdblabs/duckplyr')"


# fetch hive partitioned data
git lfs fetch --all
git lfs pull

# unzip taxi-data-2019.zip
unzip taxi-data-2019.zip