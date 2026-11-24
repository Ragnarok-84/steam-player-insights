set -euo pipefail
for attempt in $(seq 1 90); do sudo podman --version >/dev/null 2>&1 && break; sleep 2; done
sudo podman --version
cd ~/phase2-repo
sed -e 's,^FROM apache/spark:,FROM docker.io/apache/spark:,' -e '/^COPY pipeline.py/a COPY infra/ infra/\nCOPY data/target_apps.json.manifest.json data/target_apps.json.manifest.json\nRUN command -v python >/dev/null || ln -s "$(command -v python3)" /usr/local/bin/python' containers/Dockerfile.spark > /home/steam/phase2.Dockerfile
image=localhost/steam-spark:k3s-20261120-af8377d
sudo podman build --build-arg MAVEN=https://repo.maven.apache.org/maven2 --label org.opencontainers.image.revision=af8377d -f /home/steam/phase2.Dockerfile -t "$image" . 2>&1 | tee ~/phase2-image-build.log
sudo podman save --format docker-archive "$image" | gzip -1 > ~/phase2-spark-image.tar.gz.part
mv ~/phase2-spark-image.tar.gz.part ~/phase2-spark-image.tar.gz
ls -lh ~/phase2-spark-image.tar.gz
