

## Building
### Prepare buildx
This only needs to happen once
```bash
docker buildx create --use --name buildx_instance
```

### Login to docker hub
```bash
docker login
```

### Build
```bash
docker buildx use buildx_instance
docker buildx build --platform linux/amd64,linux/arm64 -t calmera/leeroy:latest --push .
```