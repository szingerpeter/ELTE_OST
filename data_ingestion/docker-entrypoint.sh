# unzip data source
echo "unzipping file"
unzip -q mounted_data/2018_Electric_power_data.zip -d data/
mv data/2018\ Electric\ power\ data data/2018_electric_power_data
# hadoop classpath
#export SPARK_DIST_CLASSPATH=$(hadoop classpath)
# preprocess python script
echo "running preprocessing script"
python src/main/python/preprocess.py
# sbt
echo "packaging scala source code"
sbt package
echo "keep container running"
/bin/bash