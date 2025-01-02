use crate::ports::incoming::message_handler::MessageHandler;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use crate::Result;
use crate::Error;
use std::io::{Read, Write};
use crate::domain::protocol::{ProtocolParser, KafkaResponse};

pub struct TcpAdapter {
    listener: TcpListener,
    message_handler: Arc<dyn MessageHandler>,
}

impl TcpAdapter {
    pub fn new(addr: &str, message_handler: Arc<dyn MessageHandler>) -> Result<Self> {
        let listener = TcpListener::bind(addr).map_err(Error::Io)?;
        Ok(Self { 
            listener, 
            message_handler,
        })
    }

    pub async fn run(&self) -> Result<()> {
        println!("Server listening on port 9092");
        let protocol_parser = ProtocolParser::new();
        
        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    self.handle_connection(stream, &protocol_parser).await?;
                }
                Err(e) => println!("Error: {}", e),
            }
        }
        Ok(())
    }

    async fn handle_connection(&self, mut stream: TcpStream, protocol_parser: &ProtocolParser) -> Result<()> {
        println!("Accepted new connection");
        
        // 1. 요청 크기 읽기
        let mut size_bytes = [0u8; 4];
        stream.read_exact(&mut size_bytes).map_err(Error::Io)?;
        let message_size = i32::from_be_bytes(size_bytes);
        
        // 2. 요청 데이터 읽기
        let mut request_data = vec![0; message_size as usize];
        stream.read_exact(&mut request_data).map_err(Error::Io)?;
        
        // 3. 프로토콜 파싱
        let request = protocol_parser.parse_request(&request_data)?;
        
        // 4. 비즈니스 로직 처리
        let response = self.message_handler.handle_request(request.header.correlation_id, request.payload).await?;
        
        // 5. 응답 인코딩 및 전송
        let kafka_response = KafkaResponse {
            correlation_id: response.correlation_id,
            payload: response.payload,
        };
        let encoded = protocol_parser.encode_response(kafka_response);
        stream.write_all(&encoded).map_err(Error::Io)?;
        
        Ok(())
    }
} 