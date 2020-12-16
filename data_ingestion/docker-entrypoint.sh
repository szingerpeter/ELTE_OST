# unzip data source
unzip -q mounted_data/2018_Electric_power_data.zip -d data/
mv data/2018\ Electric\ power\ data data/2018_electric_power_data
# hadoop classpath
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
# sbt
