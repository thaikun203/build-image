#!/bin/bash

# Kiểm tra biến môi trường để debug
echo "===============> CHECK"
echo "$SPARK_CLUSTER"

# Cấu hình timezone
echo "Asia/Ho_Chi_Minh" > /etc/timezone
ln -sf /usr/share/zoneinfo/Asia/Ho_Chi_Minh /etc/localtime

if [ "$SPARK_CLUSTER" == "MASTER" ]; then
    echo "===============> Start MASTER"
    start-master.sh -p 7077
fi

if [ "$SPARK_CLUSTER" == "WORKER" ]; then  
    echo "===============> Start WORKER"
    start-worker.sh spark://$MASTER_NODE:7077
fi


# # Bắt đầu tini để đảm bảo container được quản lý tốt
exec tini -- "$@" &

# Thiết lập MinIO alias nếu chưa có
echo "Setting MinIO alias..."
# $HOME/minio-binaries/mc alias set ${MINIO_ALIAS} ${MINIO_HOST} ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY}
mc alias set ${MINIO_ALIAS} ${MINIO_HOST} ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY}
if [ $? -ne 0 ]; then
  echo "Failed to set MinIO alias."
  exit 1
fi

# Kiểm tra xem thư mục gốc đã tồn tại trong MinIO chưa, nếu chưa thì tạo mới
echo "Checking if bucket exists in MinIO..."
# $HOME/minio-binaries/mc ls ${MINIO_ALIAS}/${MINIO_BUCKET} || mc mb ${MINIO_ALIAS}/${MINIO_BUCKET}
mc ls ${MINIO_ALIAS}/${MINIO_BUCKET} || mc mb ${MINIO_ALIAS}/${MINIO_BUCKET}
if [ $? -ne 0 ]; then
  echo "Failed to create or access MinIO bucket."
  exit 1
fi

# Bắt đầu đồng bộ thư mục với MinIO
echo "Starting to mirror local folder to MinIO..."
# $HOME/minio-binaries/mc mirror --overwrite --watch ${LOCAL_PATH} ${MINIO_ALIAS}/${MINIO_BUCKET}/${MINIO_PREFIX}
mc mirror --overwrite --watch ${LOCAL_PATH} ${MINIO_ALIAS}/${MINIO_BUCKET}/${MINIO_PREFIX}
if [ $? -ne 0 ]; then
  echo "Failed to mirror to MinIO."
  exit 1
fi

# Để container tiếp tục chạy
echo "Entrypoint setup completed. Handing over to CMD."
exec "$@"
