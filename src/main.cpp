#include <boost/asio.hpp>
#include <fstream>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <ctime>
#include <sstream>
#include <iomanip>

using boost::asio::ip::tcp;

// Estrutura do registro
#pragma pack(push, 1)
struct LogRecord {
    char sensor_id[32];
    std::time_t timestamp;
    double value;
};
#pragma pack(pop)

// Funções auxiliares para conversões
std::time_t string_to_time_t(const std::string& time_string) {
    std::tm tm = {};
    std::istringstream ss(time_string);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    return std::mktime(&tm);
}

std::string time_t_to_string(std::time_t time) {
    std::tm* tm = std::localtime(&time);
    std::ostringstream ss;
    ss << std::put_time(tm, "%Y-%m-%dT%H:%M:%S");
    return ss.str();
}

// Map para gerenciar arquivos de sensores
std::unordered_map<std::string, std::mutex> sensor_mutex_map;

// Função para registrar dados
void log_sensor_data(const std::string& sensor_id, std::time_t timestamp, double value) {
    std::lock_guard<std::mutex> lock(sensor_mutex_map[sensor_id]);
    std::ofstream ofs(sensor_id + ".log", std::ios::binary | std::ios::app);
    LogRecord record;
    strncpy(record.sensor_id, sensor_id.c_str(), sizeof(record.sensor_id));
    record.timestamp = timestamp;
    record.value = value;
    ofs.write(reinterpret_cast<const char*>(&record), sizeof(record));
}

// Função para buscar dados
std::vector<LogRecord> get_sensor_logs(const std::string& sensor_id, int n) {
    std::lock_guard<std::mutex> lock(sensor_mutex_map[sensor_id]);
    std::ifstream ifs(sensor_id + ".log", std::ios::binary);
    std::vector<LogRecord> records;

    if (!ifs) return records;

    ifs.seekg(0, std::ios::end);
    size_t file_size = ifs.tellg();
    size_t record_size = sizeof(LogRecord);
    size_t total_records = file_size / record_size;

    if (n > total_records) n = total_records;
    ifs.seekg((total_records - n) * record_size, std::ios::beg);

    for (int i = 0; i < n; ++i) {
        LogRecord record;
        ifs.read(reinterpret_cast<char*>(&record), sizeof(record));
        records.push_back(record);
    }
    return records;
}

// Sessão de conexão
class session : public std::enable_shared_from_this<session> {
public:
    session(tcp::socket socket) : socket_(std::move(socket)) {}

    void start() { read_message(); }

private:
    void read_message() {
        auto self(shared_from_this());
        boost::asio::async_read_until(socket_, buffer_, "\r\n",
            [this, self](boost::system::error_code ec, std::size_t length) {
                if (!ec) {
                    std::istream is(&buffer_);
                    std::string message;
                    std::getline(is, message);

                    if (message.starts_with("LOG|")) {
                        handle_log(message);
                    } else if (message.starts_with("GET|")) {
                        handle_get(message);
                    }
                }
            });
    }

    void handle_log(const std::string& message) {
        auto parts = message.substr(4);
        auto pos1 = parts.find('|');
        auto pos2 = parts.find('|', pos1 + 1);

        std::string sensor_id = parts.substr(0, pos1);
        std::string timestamp_str = parts.substr(pos1 + 1, pos2 - pos1 - 1);
        double value = std::stod(parts.substr(pos2 + 1));

        log_sensor_data(sensor_id, string_to_time_t(timestamp_str), value);
        read_message();
    }

    void handle_get(const std::string& message) {
        auto parts = message.substr(4);
        auto pos1 = parts.find('|');
        auto pos2 = parts.find('|', pos1 + 1);

        std::string sensor_id = parts.substr(0, pos1);
        int num_records = std::stoi(parts.substr(pos1 + 1));

        if (sensor_mutex_map.find(sensor_id) == sensor_mutex_map.end()) {
            boost::asio::write(socket_, boost::asio::buffer("ERROR|INVALID_SENSOR_ID\r\n"));
            return;
        }

        auto records = get_sensor_logs(sensor_id, num_records);
        std::ostringstream response;
        response << records.size();
        for (const auto& record : records) {
            response << ";" << time_t_to_string(record.timestamp) << "|" << record.value;
        }
        response << "\r\n";

        boost::asio::write(socket_, boost::asio::buffer(response.str()));
    }

    tcp::socket socket_;
    boost::asio::streambuf buffer_;
};

// Servidor principal
class server {
public:
    server(boost::asio::io_context& io_context, short port)
        : acceptor_(io_context, tcp::endpoint(tcp::v4(), port)) {
        accept();
    }

private:
    void accept() {
        acceptor_.async_accept(
            [this](boost::system::error_code ec, tcp::socket socket) {
                if (!ec) {
                    std::make_shared<session>(std::move(socket))->start();
                }
                accept();
            });
    }

    tcp::acceptor acceptor_;
};

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: <port>\n";
        return 1;
    }

    boost::asio::io_context io_context;
    server s(io_context, std::atoi(argv[1]));
    io_context.run();

    return 0;
}
