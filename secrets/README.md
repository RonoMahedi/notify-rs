# secrets/

Provider credentials live here and are mounted read-only into the containers at `/run/secrets`.
Everything in this folder except this README is git-ignored. Never commit keys.

## Firebase (push)

1. Firebase Console → your project → ⚙️ Project settings → **Service accounts** → **Generate new private key**.
2. Save the downloaded file here as `firebase.json`.
3. iOS only: Project settings → **Cloud Messaging** → upload your **APNs auth key (.p8)**.
4. In the project root, create `.env` (git-ignored) with:

   ```
   FCM_CREDENTIALS_FILE=/run/secrets/firebase.json
   FCM_VALIDATE_ONLY=true      # first: Firebase checks everything but delivers nothing
   ```

5. `docker compose up -d worker worker-high worker-retry worker-retry-1m worker-retry-10m worker-bulk`
6. Admin UI → **System** → Providers should show `push · fcm · validate only`.
   Send a test push; Deliveries shows the result (dead tokens are removed automatically).
7. When it looks right, set `FCM_VALIDATE_ONLY=false` and restart the workers to deliver for real.
