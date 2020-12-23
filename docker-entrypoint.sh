# unzip data source
echo "unzipping file"
unzip -q mounted_data/2018_Electric_power_data.zip -d data/
mv data/2018\ Electric\ power\ data data_ingestion/data/2018_electric_power_data
# hadoop classpath
#export SPARK_DIST_CLASSPATH=$(hadoop classpath)

# package all scala projects
# sbt
cd forecasting
echo $(pwd)
echo "packaging scala source code forecasting"
sbt package -Dsbt.rootdir=true
cd ..


cd data_ingestion
echo "running preprocessing script"
python src/main/python/preprocess.py

echo "packaging scala source code data_ignestion"
sbt package
cd ..

rmdir data


while true; do
    read -p "Do you wish bash(Bb) or fish(Ff)?" fb
    case $fb in
        [Ff]* ) apt-get install fish; fish; break;;
        [Bb]* ) /bin/bash; exit;;
        * ) echo "Please answer Bb or Ff.";;
    esac
done

