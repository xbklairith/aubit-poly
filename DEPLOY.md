# Production Deployment

## 1. Setup .env
```bash
cp .env.example .env
# Edit .env with production values
```

## 2. Start Database
```bash
docker compose -f docker-compose-db.yml up -d
```

## 3. Build & Start Services
```bash
docker compose up -d --build
```

## 4. Check Status
```bash
docker compose ps
docker compose logs -f trade-executor
```

## 5. Stop Services
```bash
docker compose down
```

## 6. Stop Database
```bash
docker compose -f docker-compose-db.yml down
```

## Live Trading
Edit `docker-compose.yml`, change `--dry-run` to `--dry-run false`
