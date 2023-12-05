# Twogether

- docker build -t gcr.io/towgether/my-api-flask-app .
- docker push gcr.io/towgether/my-api-flask-app
- gcloud run deploy my-api-flask-service --image gcr.io/towgether/my-api-flask-app --platform managed --allow-unauthenticated --project towgether --region us-central1
