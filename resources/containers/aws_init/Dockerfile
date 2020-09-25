FROM dwp-python

WORKDIR $APPLICATION_HOME
COPY *.* ./
RUN chown -R $SERVICE_USER:$SERVICE_USER .
RUN chmod +x *.sh *.py
USER $SERVICE_USER
RUN pip install -r requirements.txt
ENTRYPOINT ["./init.sh"]
