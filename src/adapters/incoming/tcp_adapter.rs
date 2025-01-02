use crate::ports::incoming::message_handler::MessageHandler;
use crate::ports::incoming::protocol_parser::ProtocolParser;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use crate::Result;
use crate::ApplicationError;
use std::io::{Read, Write};

pub struct TcpAdapter {
    listener: TcpListener,
    message_handler: Arc<dyn MessageHandler>,
    protocol_parser: Arc<dyn ProtocolParser>,
}

impl TcpAdapter {
    pub fn new(
        addr: &str, 
        message_handler: Arc<dyn MessageHandler>,
        protocol_parser: Arc<dyn ProtocolParser>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).map_err(ApplicationError::Io)?;
        Ok(Self { 
            listener,
            message_handler,
            protocol_parser,
        })
    }

    pub async fn run(&self) -> Result<()> {
        println!("Server listening on port 9092");
        
        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    self.handle_connection(stream).await?;
                }
                Err(e) => println!("Error: {}", e),
            }
        }
        Ok(())
    }

    async fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        println!("Accepted new connection");
        
        // 1. 요청 크기 읽기
        let mut size_bytes = [0u8; 4];
        stream.read_exact(&mut size_bytes).map_err(ApplicationError::Io)?;
        let message_size = i32::from_be_bytes(size_bytes);
        
        // 2. 요청 데이터 읽기
        let mut request_data = vec![0; message_size as usize];
        stream.read_exact(&mut request_data).map_err(ApplicationError::Io)?;
        
        // 3. 프로토콜 파싱
        let request = self.protocol_parser.parse_request(&request_data).map_err(ApplicationError::Domain)?;
        
        // 4. 비즈니스 로직 처리
        let response = self.message_handler.handle_request(request).await?;
        
        // 5. 응답 인코딩 및 전송
        let encoded = self.protocol_parser.encode_response(response);
        stream.write_all(&encoded).map_err(ApplicationError::Io)?;
        
        Ok(())
    }
} 