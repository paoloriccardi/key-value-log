FROM python:3.8-alpine3.13

COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
#RUN apk --no-cache add curl
EXPOSE 7001
ENTRYPOINT [ "python" ]
CMD [ "appGateway.py" ]