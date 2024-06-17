FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim

RUN pip install pipenv 

# RUN apt install s3fs

WORKDIR /app

COPY [ "Pipfile", "Pipfile.lock", "./" ]

# COPY [ "/home/ubuntu/.aws/config", "/home/ubuntu/.aws/config" ]

RUN pipenv install --system --deploy

COPY [ "starter.py", "./" ]

ENTRYPOINT [ "python", "./starter.py" ]
# docker run <imagename> --year <YYYY> --month <MM>