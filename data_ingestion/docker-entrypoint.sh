echo "unzipping file"
if ! [[ -d "data/2018_electric_power_data" ]]; then
  unzip -q mounted_data/2018_Electric_power_data.zip -d data/
  mv data/2018\ Electric\ power\ data data/2018_electric_power_data
fi


echo "running preprocessing script"
python src/main/python/preprocess.py

echo "packaging scala source code"
if ! [[ -d "target" ]]; then
  sbt package
fi

echo "waiting for kafka service"
./wait-for-it.sh kafka:9093 --timeout=100 -- echo "kafka is up"

