FROM lowcodet132/lowcode-ai:base_25082026_v1

ENV NB_USER=jovyan \
    NB_UID=1000 \
    HOME=/home/jovyan

USER root

# --- Custom wheels + Python packages ---
COPY ./dist/jupyterlab_pipeline-0.8.0-py3-none-any.whl \
     ./dist/jupyterlab_language_pack_vi_vn-4.4.post0-py2.py3-none-any.whl \
     ./dist/pipeline_db_content_manager-0.1.0-py3-none-any.whl \
     /tmp/

RUN pip install --no-cache-dir \
        /tmp/jupyterlab_pipeline-0.8.0-py3-none-any.whl \
        /tmp/pipeline_db_content_manager-0.1.0-py3-none-any.whl \
        /tmp/jupyterlab_language_pack_vi_vn-4.4.post0-py2.py3-none-any.whl \
        jmespath \
        rouge-score \
 && jupyter labextension enable jupyterlab-language-pack-vi-VN \
 && rm -f /tmp/*.whl \
 && pip check || true

# --- Jupyter config ---
RUN mkdir -p /usr/local/etc/jupyter/labconfig
COPY jupyter_server_config.py /usr/local/etc/jupyter/
COPY page_config.json /usr/local/etc/jupyter/labconfig/page_config.json

RUN mkdir -p /usr/local/share/jupyter/lab/settings
COPY overrides.json /usr/local/share/jupyter/lab/settings/overrides.json

# --- App code ---
COPY lakehouse_api.py /tmp/lakehouse_api.py
RUN mv /tmp/lakehouse_api.py \
        "$(python3 -c 'import site; print(site.getsitepackages()[0])')/" \
 && python3 -c "import lakehouse_api; print('lakehouse_api OK')"

COPY notebooks/ /tmp/sample_notebooks/
RUN chown -R ${NB_USER}:${NB_USER} /tmp/sample_notebooks ${HOME}

# --- Entrypoint ---
COPY entrypoint.sh /tmp/entrypoint.sh
RUN chmod +x /tmp/entrypoint.sh

WORKDIR ${HOME}
USER ${NB_USER}

EXPOSE 8888

ENTRYPOINT ["tini", "-s", "--", "/tmp/entrypoint.sh"]
CMD ["jupyter", "lab", "--notebook-dir=/home/jovyan"]