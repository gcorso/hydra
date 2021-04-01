openssl aes-256-cbc -in libs/db/include/db/config.h.enc -out libs/db/include/db/config.h -d -pass pass:${1} -iter 1000000
cmake .;
make;
apps/caput;

# usage: git clone https://github.com/lukecavabarrett/hydra.git; sh hydra/gpustartup.sh password
