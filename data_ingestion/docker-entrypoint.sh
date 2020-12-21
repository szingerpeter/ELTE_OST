# unzip data source
echo "unzipping file"
if ! [[ -d "data/2018_electric_power_data" ]]; then
  unzip -q mounted_data/2018_Electric_power_data.zip -d data/
  mv data/2018\ Electric\ power\ data data/2018_electric_power_data
fi
# hadoop classpath
#export SPARK_DIST_CLASSPATH=$(hadoop classpath)
# preprocess python script
echo "running preprocessing script"
python src/main/python/preprocess.py
# sbt
echo "packaging scala source code"
if ! [[ -d "target" ]]; then
  sbt package
fi
echo "start streaming"
spark-shell --packages "org.apache.spark":"spark-sql-kafka-0-10_2.12":"3.0.1" -i src/main/scala/script.scala
#echo "keep container running"
#/bin/bash
