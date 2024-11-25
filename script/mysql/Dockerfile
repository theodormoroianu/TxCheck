# Use the official Ubuntu image as the base image
FROM ubuntu:20.04

# Set environment variables for MySQL
ENV MYSQL_ROOT_PASSWORD=root
ENV MYSQL_DATABASE=testdb
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary dependencies for MySQL and TxCheck
RUN apt-get update && apt-get install -y \
    mysql-server \
    build-essential \
    autoconf \
    autoconf-archive \
    libpqxx-dev \
    libboost-regex-dev \
    libsqlite3-dev \
    libmysqlclient-dev \
    git

# Initialize the MySQL data directory
RUN rm /var/lib/mysql/ -r
RUN mysqld --initialize-insecure --user=mysql

# Set the working directory
WORKDIR /home/mysql
USER root

# Copy TxCheck source code into the container
COPY . /home/mysql/TxCheck

# Build and install TxCheck
RUN autoreconf -if /home/mysql/TxCheck && \
    cd /home/mysql/TxCheck && \
    ./configure && \
    make clean && \
    make -j


# Set the entrypoint to run TxCheck
ENTRYPOINT ["/home/mysql/TxCheck/transfuzz", "--mysql-db=testdb", "--mysql-port=3306", "--output-or-affect-num=1"]