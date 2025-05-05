from kafka import KafkaProducer, KafkaConsumer
import requests
import json
import time
from datetime import datetime
import logging

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cấu hình thông số
CONFIG = {
    "API_KEY": "...",
    "locations": [
        {"name": "Ho_Chi_Minh", "lat": "10.7758439",    "lon": "106.7017555"},
        {"name": "Ha_Noi",      "lat": "21.028511",     "lon": "105.804817"},
        {"name": "Hai_Phong",   "lat" : "20.865139",    "lon" : "106.683830"},
        {"name": "Soc_Trang",   "lat" : "9.602521",     "lon" : "105.973907"},
        {"name": "Hue",         "lat" : "16.463713",    "lon" : "107.590866"}
    ],
    "kafka_bootstrap_servers": "kafka:9092",  # Địa chỉ Kafka broker
    "kafka_topic": "weather_data",  # Tên Topic
    "normal_interval": 60,  # Thu thập mỗi 1 phút 
    "error_retry_interval": 5 , # Chờ 5 giây khi có lỗi
    "location_interval": 5  # Khoảng thời gian giữa các lần thu thập từ các địa điểm khác nhau
}

def create_kafka_producer():
    """Tạo và trả về Kafka producer"""
    # Tạo Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=CONFIG['kafka_bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        logger.info("Kafka producer được tạo thành công")
        return producer
    except Exception as e:
        logger.error(f"Lỗi khi tạo Kafka producer: {e}")
        return None

def fetch_weather_data(location):
    """Thu thập dữ liệu thời tiết từ API"""
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={location['lat']}&lon={location['lon']}&appid={CONFIG['API_KEY']}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        data['timestamp'] = datetime.now().isoformat()
        return data
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi khi kết nối API thời tiết: {e}")
        return None
    except Exception as e:
        logger.error(f"Lỗi: {e}")
        return None

def send_to_kafka(producer, data):
    """Gửi dữ liệu đến Kafka"""
    try:
        prod = producer.send(
            CONFIG['kafka_topic'],
            value=data
        )
        # Chờ xác nhận từ Kafka
        prod.get(timeout=10)
        logger.info(f"Đã gửi dữ liệu đến Kafka topic {CONFIG['kafka_topic']} || timestamp : {data['timestamp']}")
        return True
    except Exception as e:
        logger.error(f"Lỗi khi gửi dữ liệu đến Kafka: {e}")
        return False
    
def process_location(producer, location):
    """Xử lý thu thập và gửi dữ liệu cho một địa điểm"""
    weather_data = fetch_weather_data(location)
    if weather_data:
        return send_to_kafka(producer, weather_data)
    return False

def main():
    """Chương trình chính"""
    # Tạo Producer
    producer = create_kafka_producer()
    while not producer:
        time.sleep(CONFIG['error_retry_interval'])
        producer = create_kafka_producer()
    
    # Vòng lặp chính
    while True:
        try:
            for location in CONFIG['locations']:
                # Xử lý từng địa điểm
                success = process_location(producer, location)
                
                # Nếu không thành công, thử lại ngay lập tức
                if not success:
                    time.sleep(CONFIG['error_retry_interval'])
                    success = process_location(producer, location)
                
                # Chờ một khoảng thời gian ngắn giữa các địa điểm
                time.sleep(CONFIG['location_interval'])
            
            # Sau khi xử lý tất cả địa điểm, chờ interval chính
            time.sleep(CONFIG['normal_interval'])
                
        except Exception as e:
            logger.error(f"Lỗi không mong muốn: {e}")
            time.sleep(CONFIG['error_retry_interval'])


if __name__ == "__main__":
    logger.info("Bắt đầu chương trình thu thập dữ liệu thời tiết")
    main()
