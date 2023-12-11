mvn clean install -Dappname=all
mkdir -p /opt/tis/tis-datax-executor
rm -rf   /opt/tis/tis-datax-executor/*
tar xvf ../tis-datax-executor.tar.gz  -C /opt/tis/
