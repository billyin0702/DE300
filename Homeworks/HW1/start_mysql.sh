# Create mysql container
docker pull mysql
docker volume create mysql_data

# Starts the docker container containing the MySQL database
docker run --name sql_container \
  --network de300_net \
  -e MYSQL_ROOT_PASSWORD=de3002024spring \
  -e MYSQL_DATABASE=heart_disease \
  -e MYSQL_USER=de300_u1 \
  -e MYSQL_PASSWORD=de300u1springaccess \
  -v mysql_data:/var/lib/mysql \
  -d mysql \