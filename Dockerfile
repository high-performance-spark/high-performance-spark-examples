# Open JDK11, Spark 3.X and the latest JDKs get a little spicy
FROM azul/zulu-openjdk:11-latest

RUN apt-get -qq update && \
    apt-get -qq -y upgrade && \
    apt-get -qq -y --no-install-recommends install gnupg software-properties-common locales curl tzdata apt-transport-https curl gnupg sudo && \
    locale-gen en_US.UTF-8 && \
    apt-get -qq -y install gnupg software-properties-common curl git-core wget axel python3 python3-pip nano emacs vim && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import && \
    chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg && \
    apt-get update && \
    apt-get -qq -y install sbt && \
    rm -rf /var/lib/apt/lists/*

RUN curl -Lo coursier https://git.io/coursier-cli
RUN chmod +x coursier
# ensure the JAR of the CLI is in the coursier cache, in the image
RUN ./coursier --help
RUN pip install jupyter
RUN ./coursier bootstrap \
      -r jitpack \
      -i user -I user:sh.almond:scala-kernel-api_2.12.19:0.14.0-RC15 \
      sh.almond:scala-kernel_2.12.19:0.14.0-RC15 \
      --default=true --sources \
      -o almond && \
  ./almond --install --log info --metabrowse --id scala2.12 --display-name "Scala 2.12"
RUN ./coursier launch almond --scala 2.12.19 -- --install

RUN adduser dev
RUN adduser dev sudo
RUN echo 'dev:dev' | chpasswd 
RUN mkdir -p ~dev
RUN cp ./coursier ~dev/
RUN echo "color_prompt=yes" >> ~dev/.bashrc
RUN echo "export force_color_prompt=yes" >> ~dev/.bashrc
RUN echo "export SPARK_HOME=/high-performance-spark-examples/spark-3.5.1-bin-hadoop3" >> ~dev/.bashrc
RUN chown -R dev ~dev
USER dev
# Kernels are installed in user so we need to run as the user
RUN ~/coursier launch almond --scala 2.12.19 -- --install
USER root

RUN mkdir /high-performance-spark-examples
RUN chown -R dev /high-performance-spark-examples
WORKDIR /high-performance-spark-examples
# Increase the chance of caching by copying just the env setup file first.
COPY --chown=dev:dev env_setup.sh ./
# Downloads and installs Spark ~3.5 & Iceberg 1.4 and slipstreams the JAR in-place
# Also downloads some test data
RUN ./env_setup.sh
COPY misc/kernel.json ~dev/.local/share/jupyter/kernels/scala/kernel.json
RUN git clone https://github.com/holdenk/spark-upgrade.git
RUN chown -R dev /high-performance-spark-examples
ADD --chown=dev:dev myapp.tar /high-performance-spark-examples/
RUN chown -R dev /high-performance-spark-examples
USER dev
RUN sbt clean compile

