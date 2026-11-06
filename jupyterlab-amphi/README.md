# Hướng dẫn khởi chạy Jupyter ETL và Spark-Iceberg bằng Docker Compose
Cấu hình này cung cấp một môi trường JupyterLab để thực hiện ETL cùng với một container Spark-Iceberg được thiết lập để kết nối với JupyterLab. Thư mục làm việc sẽ được mount vào container JupyterLab để có thể chỉnh sửa mã nguồn từ bên ngoài.

## Yêu cầu
Docker

Docker Compose
## Build Images
1. cd vào thư mục jupyterlab-amphi

```bash
cd jupyterlab-amphi
```
2. chạy lệnh build imgaes

```bash
docker build -t [image_name]:[tag] .
```

vd: docker build -t jupyter_etl:1.0 .

## Hướng dẫn

1. Khởi động các dịch vụ
Vào trong file docker-compose.yaml và sửa lại tên image thành tên image vừa build ở bước trên

Chạy lệnh sau để khởi động các container:

```bash
docker-compose up -d
```
Lệnh này sẽ khởi động các dịch vụ sau:

JupyterLab tại http://localhost:8888
Spark-Iceberg với Spark master tại spark://spark-iceberg:7077

2. Truy cập JupyterLab
Mở trình duyệt và truy cập http://localhost:8888.
Vào trong log của container jupyter_etl sau đó copy token và paste vào phần xác thực trên giao diện
Thư mục jupyterlab-amphi đã được mount vào container JupyterLab, cho phép code trực tiếp từ môi trường bên ngoài mà vẫn thấy được trong JupyterLab.

3. Truy cập và cấu hình Spark-Iceberg
Để khởi chạy một Spark worker trong container spark-iceberg, thực hiện các bước sau:

Mở terminal và truy cập vào container spark-iceberg:

```bash
docker exec -it spark-iceberg bash
```
Điều hướng đến thư mục bin:

```bash
cd /opt/spark/sbin
```
Khởi chạy Spark worker và kết nối với Spark master tại spark://spark-iceberg:7077:

```bash
./start-worker.sh spark://spark-iceberg:7077
```
Thao tác này sẽ khởi động Spark worker và kết nối với Spark master, cho phép xử lý phân tán với Spark.