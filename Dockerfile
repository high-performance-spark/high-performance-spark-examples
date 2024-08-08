# Open JDK11, Spark 3.X and the latest JDKs get a little spicy
FROM azul/zulu-openjdk:11-latest

RUN apt-get -qq update && \
    apt-get -qq -y upgrade && \
    apt-get -qq -y --no-install-recommends install gnupg software-properties-common locales curl tzdata apt-transport-https curl gnupg sudo && \
    locale-gen en_US.UTF-8 && \
    apt-get -qq -y install gnupg software-properties-common curl git-core wget axel && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import && \
    chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg && \
    apt-get update && \
    apt-get -qq -y install sbt && \
    rm -rf /var/lib/apt/lists/*

RUN useradd dev
RUN mkdir /high-performance-spark-examples
RUN chown -R dev high-performance-spark-examples
WORKDIR /high-performance-spark-examples
# Increase the chance of caching by copying just the env setup file first.
COPY --chown=dev:dev env_setup.sh ./
USER dev
RUN ./env_setup.sh
ADD --chown=dev:dev myapp.tar .
RUN sbt clean compile
# Downloads and installs Spark ~3.5 & Iceberg 1.4 and slipstreams the JAR in-place
# Also downloads some test data
RUN ./env_setup.sh
