FROM ubuntu:20.04 
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y apt-utils
RUN apt-get install -y build-essential python3-dev automake git flex bison libglib2.0-dev libpixman-1-dev python3-setuptools
RUN apt-get install -y vim cmake curl wget gdb

# for mariadb
RUN apt-get -y update
RUN apt-get -y install software-properties-common devscripts equivs
# Install dependencies.
RUN apt-get update \
    && apt-get install -y \
        git \
        gzip \
        tar \
        gcc \
        g++ \
        make \
        bison \
        libncurses5-dev \
        zlib1g-dev \
        libevent-dev \
        cmake \
        libgnutls28-dev \
        libssl-dev \
        libjemalloc-dev \
        libsnappy-dev \
        valgrind \
        libcurl4-openssl-dev \
        libxml2-dev \
        libboost-all-dev \
        libaio-dev \
        libsystemd-dev \
        libpcre2-dev \
        ccache \
    && apt-get clean


# add user named mysql
RUN useradd -ms /bin/bash mysql

USER mysql
WORKDIR /home/mysql
RUN git clone --branch mariadb-10.8.3 https://github.com/MariaDB/server.git
RUN mv server/ mariadb/

USER mysql
WORKDIR /home/mysql/mariadb
RUN git submodule update --init --recursive
RUN mkdir /home/mysql/mariadb/build
WORKDIR /home/mysql/mariadb/build
RUN cmake .. -DCMAKE_BUILD_TYPE=Debug
RUN make -j8
USER root
RUN git config --global --add safe.directory /home/mysql/mariadb
RUN make install
WORKDIR /usr/local/mysql/
RUN scripts/mysql_install_db
RUN chown -R mysql:mysql /usr/local/mysql/
# add soft link in /usr/lib/ for libmysqlclient.so
USER root
WORKDIR /usr/lib
RUN ln -s /usr/local/mysql/lib/libmysqlclient.so libmysqlclient.so

# get TxCheck
USER  root
RUN apt-get update
RUN apt-get install -y build-essential autoconf autoconf-archive libpqxx-dev libboost-regex-dev libsqlite3-dev
WORKDIR /home/mysql
# RUN git clone https://github.com/JZuming/TxCheck
COPY . /home/mysql/TxCheck
RUN ls -l /home/mysql/TxCheck
WORKDIR /home/mysql/TxCheck
RUN autoreconf -if
# RUN ./configure
run ./configure CXXFLAGS=-I/usr/local/mysql/include/
RUN make clean; make -j8

USER  root
WORKDIR /home/mysql
ENTRYPOINT ["/home/mysql/TxCheck/transfuzz", "--mariadb-db=testdb", "--mariadb-port=3306", "--output-or-affect-num=0"]