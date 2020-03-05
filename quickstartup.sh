sudo apt-get update -y
sudo apt-get install -y libpthread-stubs0-dev postgresql libpq-dev libpqxx-dev libboost-all-dev
git pull;
cmake .;
make;
openssl aes-256-cbc -in config.ini.enc -out config.ini -d -pass pass:${1} -iter 1546
apps/hydra;

# usage: git clone https://github.com/lukecavabarrett/hydra.git; cd hydra; git checkout dev; sh quickstartup.sh password
