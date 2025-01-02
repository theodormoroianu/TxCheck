# Use the official Ubuntu image as the base image
FROM ubuntu:20.04

# Set environment variables for MariaDB
ENV MARIADB_ROOT_PASSWORD=root
ENV MARIADB_DATABASE=testdb
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary dependencies for MariaDB and TxCheck
RUN apt-get update && apt-get install -y \
    mariadb-server \
    build-essential \
    autoconf \
    autoconf-archive \
    libpqxx-dev \
    libboost-regex-dev \
    libsqlite3-dev \
    libmariadb-dev \
    git

RUN ln -s /usr/lib/x86_64-linux-gnu/libmariadb.so /usr/lib/x86_64-linux-gnu/libmysqlclient.so && \
    ln -s /usr/lib/x86_64-linux-gnu/libmariadb.so.3 /usr/lib/x86_64-linux-gnu/libmysqlclient.so.18 && \
    rm /usr/lib/x86_64-linux-gnu/libmysqlclient.so && \
    ln -s /usr/lib/x86_64-linux-gnu/libmariadbclient.so /usr/lib/x86_64-linux-gnu/libmysqlclient.so
# Ensure the MariaDB shared libraries are in the correct location
RUN ldconfig

# Initialize the MariaDB data directory
RUN rm /var/lib/mysql/ -r
RUN mysql_install_db

# Set the working directory
WORKDIR /home/mysql
USER root

# Copy TxCheck source code into the container
COPY . /home/mysql/TxCheck


# Build and install TxCheck
RUN autoreconf -if /home/mysql/TxCheck && \
    cd /home/mysql/TxCheck && \
    ./configure CXXFLAGS="-I/usr/include/mariadb/" LDFLAGS="-lmariadb" && \
    make clean && \
    make -j

# Set the entrypoint to run TxCheck
ENTRYPOINT ["/home/mysql/TxCheck/transfuzz", "--mariadb-db=testdb", "--mariadb-port=3306", "--output-or-affect-num=1"]
