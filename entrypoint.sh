#!/bin/bash

# Kiểm tra biến môi trường để debug
echo "===============> CHECK"
echo "$SPARK_CLUSTER"

# Cấu hình timezone
echo "Asia/Ho_Chi_Minh" > /etc/timezone
ln -sf /usr/share/zoneinfo/Asia/Ho_Chi_Minh /etc/localtime

# Tạo LOCAL_PATH nếu chưa tồn tại và cấp quyền
if [ -z "$LOCAL_PATH" ]; then
  echo "Environment variable LOCAL_PATH is not set. Setting default value..."
  LOCAL_PATH="/home/jovyan"
fi

echo "Ensuring LOCAL_PATH exists and setting permissions: $LOCAL_PATH"
mkdir -p "$LOCAL_PATH"
if [ $? -ne 0 ]; then
  echo "Failed to create LOCAL_PATH: $LOCAL_PATH"
  exit 1
fi

chmod 777 "$LOCAL_PATH"
if [ $? -ne 0 ]; then
  echo "Failed to set permissions for LOCAL_PATH: $LOCAL_PATH"
  exit 1
fi

if [ "$SPARK_CLUSTER" == "MASTER" ]; then
    echo "===============> Start MASTER"
    start-master.sh -p 7077
fi

if [ "$SPARK_CLUSTER" == "WORKER" ]; then  
    echo "===============> Start WORKER"
    start-worker.sh spark://$MASTER_NODE:7077
fi

# Bắt đầu tini để đảm bảo container được quản lý tốt
exec tini -- "$@" &

# Thiết lập MinIO alias nếu chưa có


# Để container tiếp tục chạy
echo "Entrypoint setup completed. Handing over to CMD."
exec "$@"
