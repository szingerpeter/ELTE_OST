# sbt
echo "packaging scala source code"
sbt package -Dsbt.rootdir=true
echo "running scala program"
sbt run -Dsbt.rootdir=true
echo "exiting docker-entrypoint.sh"
