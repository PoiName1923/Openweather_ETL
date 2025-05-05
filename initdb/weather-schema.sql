-- Bảng thành phố (dimension table)
CREATE TABLE cities (
    city_id INT PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    latitude DECIMAL(9,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL
);

-- Bảng thời tiết (fact table)
CREATE TABLE weather_measurements (
    measurement_id BIGSERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    -- Thông số thời tiết
    temperature_celsius DECIMAL(4,1) NOT NULL,
    feels_like_celsius DECIMAL(4,1) NOT NULL,
    temp_min_celsius DECIMAL(4,1) NOT NULL,
    temp_max_celsius DECIMAL(4,1) NOT NULL,
    pressure_hpa INT NOT NULL,
    humidity_percent INT NOT NULL,
    -- Thông tin gió
    wind_speed_mps DECIMAL(5,2) NOT NULL,
    wind_direction_deg INT,
    wind_gust_mps DECIMAL(5,2),
    -- Lượng mưa
    rain_1h_mm DECIMAL(5,2) DEFAULT 0,
    -- Mây
    cloud_coverage_percent INT NOT NULL,
    -- Điều kiện thời tiết
    weather_condition VARCHAR(50) NOT NULL,
    weather_description VARCHAR(100) NOT NULL,
    -- Mặt trời
    sunrise_time TIMESTAMP,
    sunset_time TIMESTAMP,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

-- Bảng tổng hợp hàng ngày
CREATE TABLE weather_daily_aggregates (
    id BIGSERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    aggregate_date DATE NOT NULL,
    -- Thống kê nhiệt độ
    avg_temperature DECIMAL(4,1) NOT NULL,
    max_temperature DECIMAL(4,1) NOT NULL,
    min_temperature DECIMAL(4,1) NOT NULL,
    -- Thống kê nhiệt độ cảm nhận
    avg_feels_like DECIMAL(4,1) NOT NULL,
    max_feels_like DECIMAL(4,1) NOT NULL,
    min_feels_like DECIMAL(4,1) NOT NULL,
    -- Thống kê áp suất
    avg_pressure DECIMAL(8,2) NOT NULL,
    max_pressure DECIMAL(8,2) NOT NULL,
    min_pressure DECIMAL(8,2) NOT NULL,
    -- Thống kê độ ẩm
    avg_humidity DECIMAL(5,2) NOT NULL,
    max_humidity DECIMAL(5,2) NOT NULL,
    min_humidity DECIMAL(5,2) NOT NULL,
    -- Thống kê gió
    avg_wind_speed DECIMAL(5,2) NOT NULL,
    max_wind_speed DECIMAL(5,2) NOT NULL,
    min_wind_speed DECIMAL(5,2) NOT NULL,
    max_wind_gust DECIMAL(5,2),
    -- Tổng lượng mưa
    total_rainfall DECIMAL(8,2) DEFAULT 0,
    -- Số lần đo
    measurement_count INT NOT NULL,
    -- Metadata
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (city_id) REFERENCES cities(city_id),
    UNIQUE (city_id, aggregate_date)  -- Đảm bảo mỗi thành phố chỉ có 1 bản ghi mỗi ngày
);