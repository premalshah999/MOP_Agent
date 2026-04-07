# MOP Frontend

React + TypeScript + Vite client for the MOP Research Agent.

## Commands

```bash
npm install
npm run dev
npm run typecheck
npm run build
npm run preview
```

## API wiring

- Dev mode uses Vite proxy (`/api`, `/health`) -> `http://127.0.0.1:8000`.
- For deployed split-host setups, set:

```env
VITE_API_BASE_URL=https://your-api-domain.com
```
