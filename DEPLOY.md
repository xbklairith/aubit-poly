# Production Deployment

## 1. Start Database
```bash
docker-compose -f docker-compose-db.yml up -d
```

## 2. Build & Start Services
```bash
docker-compose up -d --build
```

## 3. Check Status
```bash
docker-compose ps
docker-compose logs -f trade-executor
```

## 4. Stop Services
```bash
docker-compose down
```

## 5. Stop Database
```bash
docker-compose -f docker-compose-db.yml down
```

## Live Trading
Edit `docker-compose.yml`, change `--dry-run` to `--dry-run false`
