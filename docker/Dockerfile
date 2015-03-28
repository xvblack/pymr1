FROM xvblack/build-essential


RUN apt-get update

RUN apt-get install -y \
	git \
	python \
	python-pip \
    python-dev
WORKDIR /home/pymr1

RUN easy_install -U pip

RUN apt-get install -y \
    libffi-dev

RUN pip install \
	setuptools \
	docker-py \
	pathlib \
	atomic

RUN ["git", "clone", "https://github.com/xvblack/pymr1"]
RUN ["git", "clone", "-b", "pymr1", "https://github.com/xvblack/thriftpy"]

WORKDIR /home/pymr1/pymr1

WORKDIR /home/pymr1/thriftpy

RUN ["python", "setup.py", "develop"]

RUN ["mkdir", "/tmp/container"]

WORKDIR /home/pymr1/pymr1

CMD ["/bin/bash"]
