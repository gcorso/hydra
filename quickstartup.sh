sudo apt-get update -y
sudo apt-get install -y libpthread-stubs0-dev libpq-dev libpqxx-dev libboost-all-dev
git pull;
cmake .;
make;
apps/sample;

# usage: git clone https://github.com/lukecavabarrett/hydra.git; cd hydra; git checkout dev; sh quickstartup.sh password
