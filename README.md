# Sound TxCheck

## Description

This project is a modification of the `TxCheck` fuzzing tool, which includes a sound DSG extraction method.

A description of what the modifications to this tool try to accomplish can be found in my thesis report, which is available [here](https://github.com/theodormoroianu/MasterThesisReport/blob/master/thesis.pdf).

## Build

The build process is the same as the original `TxCheck` tool. The instructions for building the tool can be found in the original README file, which is included below.

However, due to the slow compilation time of DBMS systems, I opted to update the dockerfiles to include precompiled versions of the DBMS systems. The scripts for building mysql (the only one I tested the latest version of) can be found in the `script` directory.

Finding the exact shared libraries to link against can be a bit annoying. The command I used when building the tool for `mysql` was:

```shell
autoreconf -if && ./configure LDFLAGS="-L/usr/lib64/mysql -I/home/theodor/Projects/libpqxx-6.4.7/include" && make -j
```

Please check the build instructions in the original README for more information, as the build process is the same. Note that if you are planning to run the tool in a docker container, you do not need to build it, as the docker build file will take care of that.

## Usage

After building the tool, you can use it in the same way as the original `TxCheck` tool.
For testing the tool for `mysql`, use the following steps:

1. Make sure that the binary paths specified in `mysql.cc` (e.g., `mysql`, `mysqldump`, `mysqladmin`) are correct for your local system.
2. Build the tool using the instructions in the original README.
3. (optional) start a `mysql` server on your local machine (this avoids having to wait for the tool to spawn a new server every time):
    ```shell
    docker run -d -p 3306:3306 --env MYSQL_ALLOW_EMPTY_PASSWORD=yes mysql:latest
    ```
4. Run the tool:
    ```shell
    ./transfuzz --mysql-db=testdb --mysql-port=3306 --output-or-affect-num=1
    ```

## Changes from the original tool

The changes made to the original tool are as follows:

1. I updated some dependencies, to make the tool compile on my system.
2. I changed the instrumentation code to add `Before Predicate Match`, `After Predicate Match` and `Predicate Match` statements. The main changes were made to the `instrumentor.cc` file.
3. I added the dependency extraction code to handle the overwrite dependencies. The main changes were made to the `dependency_analyzer.cc` file.
4. I updated a bit the rest of the code to make stuff work.

If you really want to see the changes, I recommend using a diff tool to compare the original `TxCheck` tool with this one.


# Original Markdown README of TxCheck

## Description

TxCheck is a tool for finding transactional bugs in database management systems. It uses SQL-level instrumentation to capture statement-level dependencies and construct transactional oracle to find bugs in transaction supports of DBMSs. We implemented TxCheck on the top of [SQLsmith](https://github.com/anse1/sqlsmith).

The [paper](https://jzuming.github.io/paper/osdi23-jiang.pdf) based on this tool has been accepted by [OSDI 2023](https://www.usenix.org/conference/osdi23/presentation/jiang).

## Supported DBMSs
- MySQL
- MariaDB
- TiDB

## Video

We provide a video introducing the tool. The video is available in the [artifact](https://zenodo.org/record/7859034#.ZEbk2s5By4Q) stored in Zenodo.

## Quick Start or Evaluation (in Docker)

We provide scripts to quickly set up the necessary environments and test specific DBMSs using TxCheck. We recommend you follow the instructions in the scripts to evaluate TxCheck or familiarize yourself with TxCheck.

- [Test MySQL 8.0.28](./docs/mysql_test.md)
- [Test MariaDB 10.8.3](./docs/mariadb_test.md)
- [Test TiDB 5.4.0](./docs/tidb_test.md)

By following the scripts, TxCheck can find the bugs listed in [Found Bugs](#found-bugs) (given enough time). For quick evaluation, you could use the above MySQL script to set up the testing, where TxCheck might find a transactional bug in MySQL 8.0.28 within 15 minutes. 

## Make TxCheck scripts work

1. Install podman.
2. Update the config to use the correct registry:
file `/etc/containers/registries.conf`
```
unqualified-search-registries = ["docker.io"]
```


## Build TxCheck on Fedora

Make sure that `mysql`, `mysql-devel` and other required packages are installed (the compiler will complain otherwise).

The latest versions (`7.x`) of `pqxx` does not support `C++11`, so we need to install an older version of `pqxx`.

```shell
wget https://github.com/jtv/libpqxx/archive/refs/tags/6.4.7.zip
unzip 6.4.7.zip
cd libpqxx-6.4.7
./configure --disable-documentation
make -j8
sudo make install
```

For building `TxCheck`, we need to add a searchpath for the `mysql` header files.
```shell
autoreconf -if
./configure LDFLAGS="-L/usr/lib64/mysql"
make -j
```


## Build TxCheck in Debian

```shell
apt-get install -y g++ build-essential autoconf autoconf-archive libboost-regex-dev
git clone https://github.com/JZuming/TxCheck.git
cd TxCheck
autoreconf -if
./configure
make -j
```

## Usage
### Test DBMSs
```shell
# test MySQL
./transfuzz --mysql-db=testdb --mysql-port=3306 --output-or-affect-num=1
# test MariaDB
./transfuzz --mariadb-db=testdb --mariadb-port=3306 --output-or-affect-num=1
# test TiDB
./transfuzz --tidb-db=testdb --tidb-port=4000 --output-or-affect-num=1

# reproduce a found bug in MySQl and minimize the test case
./transfuzz --mysql-db=testdb --mysql-port=3306 \
            --reproduce-sql=final_stmts.sql \
            --reproduce-tid=final_tid.txt \
            --reproduce-usage=final_stmt_use.txt \
            --reproduce-backup=mysql_bk.sql \
            --min
```
The bugs found are stored in the directory `found_bugs`. TxCheck only supports testing local database engines now.

### Supported Options

| Option | Description |
|----------|----------|
| `--mysql-db` | Target MySQL database | 
| `--mysql-port` | MySQL server port number | 
| `--mariadb-db` | Target MariaDB database |
| `--mariadb-port` | Mariadb server port number |
| `--tidb-db` | Target TiDB database |
| `--tidb-port` | TiDB server port number |
| `--output-or-affect-num` | Generated statement should output or affect at least a specific number of rows |
| `--reproduce-sql` | A SQL file recording the executed statements (needed for reproducing)|
| `--reproduce-tid` | A file recording the transaction id of each statement (needed for reproducing)|
| `--reproduce-usage` | A file recording the type of each statement (needed for reproducing)|
| `--reproduce-backup` | A backup file (needed for reproducing)|
| `--min` | Minimize the bug-triggering test case|

***Note***

Both target database and the server port number should be specified (e.g., when testing MySQL or reproducing a bug in MySQL, `--mysql-db` and `--mysql-port` should be specified).

The options `--reproduce-sql`, `--reproduce-tid`, `--reproduce-usage`, and `--reproduce-backup` should be specified when TxCheck is used to reproduce a found bug. The files used are the files stored in the directory `found_bugs`. The option `--min` can work only when the options `--reproduce-sql`, `--reproduce-tid`, `--reproduce-usage` and `--reproduce-backup` are specified.

## Source Code Structure

| Source File | Description |
|----------|----------|
| `instrumentor.cc (.hh)` | SQL-level instrumentation to extract dependency information |
| `dependency_analyzer.cc (.hh)` | Build statement dependency graphs, maintain graph-related meta data (e.g. topological sorting on graphs, graph decycling)
| `transaction_test.cc (.hh)` | Manage the whole transaction-testing procedure, including transaction test-case generation, blocking scheduling, transaction-oracle checking, e.t.c.|
| `general_process.cc (.hh)` | Provide general functionality (e.g., hash functions, result-comparison methonds, SQL statement generation) |
| `dbms_info.cc (.hh)` | Maintain the information of supported DBMSs (e.g., tested db, server port number)
| `transfuzz.cc` | Maintain the program entry |
| `mysql.cc (.hh)` | Provide the functionality related to MySQL |
| `mariadb.cc (.hh)` | Provide the functionality related to MariaDB |
| `tidb.cc (.hh)` | Provide the functionality related to TiDB |
| Others | Similar to the ones in SQLsmith. We support more SQL features in `grammar.cc (.hh)` and `expr.cc (.hh)`|


## Found Bugs
- [Reported bugs in MySQL](./docs/mysql_bugs.md)
- [Reported bugs in MariaDB](./docs/mariadb_bugs.md)
- [Reported bugs in TiDB](./docs/tidb_bugs.md)
