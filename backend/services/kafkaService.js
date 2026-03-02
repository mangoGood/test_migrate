const kafka = require('kafka-node');

class KafkaService {
    constructor() {
        this.client = null;
        this.producer = null;
        this.connected = false;
        this.init();
    }

    // 初始化 Kafka 连接
    init() {
        try {
            // 创建 Kafka 客户端
            this.client = new kafka.KafkaClient({
                kafkaHost: 'localhost:9092' // Kafka 服务器地址
            });

            // 创建生产者
            this.producer = new kafka.Producer(this.client);

            // 监听连接事件
            this.producer.on('ready', () => {
                this.connected = true;
                console.log('Kafka 生产者连接成功');
            });

            // 监听错误事件
            this.producer.on('error', (err) => {
                this.connected = false;
                console.error('Kafka 生产者错误:', err);
            });

        } catch (error) {
            console.error('Kafka 初始化失败:', error);
            this.connected = false;
        }
    }

    // 发送消息到 Kafka
    sendMessage(topic, message) {
        return new Promise((resolve, reject) => {
            if (!this.connected || !this.producer) {
                console.warn('Kafka 未连接，消息发送失败');
                return resolve(false);
            }

            const payloads = [{
                topic: topic,
                messages: JSON.stringify(message),
                partition: 0
            }];

            this.producer.send(payloads, (err, data) => {
                if (err) {
                    console.error('Kafka 消息发送失败:', err);
                    reject(err);
                } else {
                    console.log('Kafka 消息发送成功:', data);
                    resolve(true);
                }
            });
        });
    }

    // 关闭连接
    close() {
        if (this.client) {
            this.client.close();
        }
    }
}

// 单例模式
const kafkaService = new KafkaService();

module.exports = kafkaService;