FROM docker.io/bitnami/spark:3
USER root
RUN useradd -ms /bin/bash notebook
USER notebook
WORKDIR /home/notebook
ENV HOME /home/notebook
ENV PATH ${PATH}:/home/notebook/.local/bin
RUN pip install --user notebook findspark pyspark-kernel spylon-kernel
COPY notebook-config.py /home/notebook/.jupyter/jupyter_notebook_config.py
RUN mkdir /home/notebook/notebooks
ENTRYPOINT []
CMD ["jupyter", "notebook"]
