## How to run

- Build the image: `docker build -t forecasting .`
- Run the image: `docker run -it --name test test bash` and run `bash docker-entrypoint.sh` (wait until it finishes)
- Run the flink program: `sbt run -Dsbt.rootdir=true`
