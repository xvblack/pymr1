FROM xvblack/build-essential


RUN apt-get update

RUN apt-get install -y \
	git \
	python \
	python-pip

WORKDIR /home/pymr1
RUN ["git", "clone", "https://github.com/xvblack/pymr1"]

WORKDIR /home/pymr1/pymr1

RUN pip install \
	setuptools \
	docker-py

WORKDIR /home/pymr1/pymr1/thriftpy

RUN ["python", "setup.py", "develop"]

WORKDIR /home/pymr1/pymr1


CMD ["/bin/bash"]