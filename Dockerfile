# Open JDK11, Spark 3.X and the latest JDKs get a little spicy
FROM azul/zulu-openjdk:11-latest

RUN apt-get -qq update && \
    apt-get -qq -y upgrade && \
    apt-get -qq -y install gnupg software-properties-common locales curl tzdata apt-transport-https curl gnupg sudo net-tools psmisc htop && \
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
      -i user -I user:sh.almond:scala-kernel-api_2.13.8:0.14.0-RC4 \
      sh.almond:scala-kernel_2.13.8:0.14.0-RC4 \
      --default=true --sources \
      -o almond && \
    ./almond --install --log info --metabrowse --id scala2.13 --display-name "Scala 2.13"
RUN chmod a+xr almond coursier
RUN ./coursier launch almond --scala 2.13.8 -- --install
# Fun story: this does not work (Aug 8 2024) because it tries to download Scala 2 from Scala 3
#RUN ./coursier install scala:2.13.8 && ./coursier install scalac:2.13.8
RUN (axel https://downloads.lightbend.com/scala/2.13.8/scala-2.13.8.deb || wget https://downloads.lightbend.com/scala/2.13.8/scala-2.13.8.deb) && dpkg --install scala-2.13.8.deb && rm scala-2.13.8.deb

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
RUN ./almond --install --log info --metabrowse --id scala2.13 --display-name "Scala 2.13"
RUN ./coursier launch almond --scala 2.13.8 -- --install
USER root

RUN mkdir /high-performance-spark-examples
RUN chown -R dev /high-performance-spark-examples
WORKDIR /high-performance-spark-examples
# Increase the chance of caching by copying just the env setup file first.
COPY --chown=dev:dev env_setup.sh ./
# Downloads and installs Spark ~3.5 & Iceberg 1.4 and slipstreams the JAR in-place
# Also downloads some test data
RUN SCALA_VERSION=2.13 ./env_setup.sh
RUN mv ~dev/.local/share/jupyter/kernels/scala2.13/kernel.json ~dev/.local/share/jupyter/kernels/scala2.13/kernel.json_back
# Note: We need to use /home in the COPY otherwise no happy pandas
COPY --chown=dev:dev misc/kernel.json /home/dev/kernel.json_new
RUN mv ~dev/kernel.json_new ~dev/.local/share/jupyter/kernels/scala2.13/kernel.json
RUN git clone https://github.com/holdenk/spark-upgrade.git
RUN chown -R dev /high-performance-spark-examples
ADD --chown=dev:dev myapp.tar /high-performance-spark-examples/
RUN chown -R dev /high-performance-spark-examples
USER dev
RUN echo "jupyter-lab --ip 0.0.0.0 --port 8877" >> ~/.bash_history
RUN sbt clean compile
CMD ["jupyter-lab", "--ip", "0.0.0.0", "--port", "8877"]

