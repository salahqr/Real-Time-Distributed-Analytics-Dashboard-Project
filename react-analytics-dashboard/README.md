# Analytics Dashboard (React)

This is a React (Vite + TypeScript) port of the Angular project you shared.

## Run

```bash
npm install
cp .env.example .env
npm run dev
```

- Set `VITE_API_URL` in `.env` to your backend base URL (example: `http://127.0.0.1:8000/api`).

## Pages
- `/` Home
- `/login` Login
- `/register` Register
- `/password/forgot` Forgot password
- `/password/reset/:token` Reset password
- `/dashboard` Analytics dashboard (calls `GET /api/user/{id}/analytics` using logged-in user id)
- `/users` Users list (calls `GET /api/users`)
- `/setup` Integration setup (sends a test event to `POST /api/track`)
