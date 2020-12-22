# sbt
echo "packaging scala source code"
sbt package -Dsbt.rootdir=true
echo "keep container running"
/bin/bash
