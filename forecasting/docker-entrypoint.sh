# sbt
echo "packaging scala source code"
sbt package
echo "running scala program"
sbt run
echo "exiting docker-entrypoint.sh"
