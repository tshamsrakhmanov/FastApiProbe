FROM python:3.12
WORKDIR /code
COPY fastapi/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
COPY fastapi/main.py /code/
CMD ["python", "main.py"]
ENTRYPOINT ["python","main.py"]