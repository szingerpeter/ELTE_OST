# sbt
# preprocess python script
echo "running preprocessing script"
python src/main/python/preprocess.py


echo "packaging scala source code data_ignestion"
sbt package