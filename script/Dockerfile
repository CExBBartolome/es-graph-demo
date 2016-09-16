FROM python:2.7.12

RUN pip install elasticsearch
RUN apt-get update && apt-get install -y vim

WORKDIR /src

COPY /src /src

CMD ["/bin/bash"]
