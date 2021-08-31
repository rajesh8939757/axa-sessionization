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
do_session.py <input_data_path> <output_data_path>
```

And you can find the results inside the outdata folder
please make sure to create folder as follows.

```bash
axa-sessinization/
|--data/
   |--input_file.csv
|--outdata/
```

And also keep add the following JVM options to `SPARK SUBMIT` 
to collect the logs to central location.
```bash
-Dlog4j.configuration=file:log4j.properties
-Dlogfile.name=axa-sessionization
-Dspark.yarn.app.container.log.dir=app-logs
```

