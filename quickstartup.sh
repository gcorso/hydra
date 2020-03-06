sudo apt-get update -y
sudo apt-get install -y libpthread-stubs0-dev libpq-dev  libboost-all-dev
git pull;
#openssl aes-256-cbc -in libs/db/include/db/config_ubuntu.h -out libs/db/include/db/config.h.enc -pass pass:${1} -iter 1000000
openssl aes-256-cbc -in libs/db/include/db/config.h.enc -out libs/db/include/db/config.h -d -pass pass:${1} -iter 1000000
cmake .;
make;
apps/caput;

# usage: git clone https://github.com/lukecavabarrett/hydra.git; cd hydra; git checkout dev; sh quickstartup.sh password
