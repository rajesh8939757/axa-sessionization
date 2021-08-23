# Web Log Analysis
This project is developed to calculate the top 10 sessions of users who downloaded the files.

## Running the job

Assuming that the `$SPARK_HOME` environment variable points to local spark installtion folder.

```bash
mkdir -p 'path/projects'
cd 'path/projects'
git clone https://github.com/rajesh8939757/axa-sessionization.git
cd axa-sessionization
```

```bash
$SPARK_HOME/bin/spark-submit \
--master local[*] \
--py-files packages.zip \
jobs/do_session.py
```
And you can find the inside the outdata folder
please make sure to create folder as follows
```bash
axa-sessinization/
|--data/
   |--input_file.csv
|--outdata/
   |--output/
```

