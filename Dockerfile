FROM quay.io/astronomer/astro-runtime:7.0.0
ENV JAVA_HOME /usr/lib/jvm/java-1.11.0-openjdk-amd64
ENV AWS_ACCESS_KEY  '[YOUR_AWS_ACCESS_KEY]'
ENV AWS_SECRET_ACCESS_KEY '[YOUR_AWS_SECRET_ACCESS_KEY]'
RUN rm -rf /app/*
COPY /app/ /app
RUN rm -rf /include/*
COPY /include/ /include
USER root
RUN chmod -R 777 /app