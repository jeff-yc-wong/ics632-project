# using base ubuntu 20.04 image
FROM ubuntu:20.04

# installing python and pip
RUN apt-get update && apt-get install -y python3 python3-pip build-essential gcc wget libfreetype6

# install dask and astropy
RUN python3 -m pip install "dask[complete]" "astropy"

# copy files into container
COPY Montage/ /bin/Montage

# add a user
RUN useradd -mrs /bin/bash -g root user

# add montage executables to $PATH
ENV PATH="/bin/Montage/bin:${PATH}"

# set home directory
ENV HOME="/home/user"

USER user
WORKDIR /home/user

CMD bash
