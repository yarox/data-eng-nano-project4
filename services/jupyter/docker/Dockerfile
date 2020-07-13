FROM jupyter/pyspark-notebook

COPY ./services/jupyter/config/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

COPY ./services/jupyter/src/ /home/jovyan/work/
