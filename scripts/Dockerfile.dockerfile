FROM python:3.11

WORKDIR /app
COPY . .

RUN pip install -r requirements.txt

ENV PORT=8080
CMD ["functions-framework", "--target=classify_and_route_file"]
