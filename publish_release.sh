# Usage: ./public_release.sh [VERSION]
docker build -f Dockerfile-worker -t creativecommons/image_crawler_worker:$1 .
docker push creativecommons/image_crawler_worker:$1
docker build -f Dockerfile-monitor -t creativecommons/image_crawler_monitor:$1
docker push creativecommons/image_crawler_monitor:$1
