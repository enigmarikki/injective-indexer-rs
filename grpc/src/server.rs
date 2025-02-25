use tonic::{Request, Response, Status};
use uuid::Uuid;
use std::sync::Arc;
use tokio::sync::mpsc;
use chrono::Utc;
use crate::producer::MessageProducer;
use crate::models::Event;

use crate::proto::{
    data_service_server::{DataService, DataServiceServer},
    DataRequest, DataResponse,
};

pub struct GrpcServer<P: MessageProducer> {
    producer: Arc<P>,
}

impl<P: MessageProducer> GrpcServer<P> {
    pub fn new(producer: Arc<P>) -> Self {
        Self { producer }
    }
    
    pub fn create_service(self) -> DataServiceServer<Self> {
        DataServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl<P: MessageProducer + 'static> DataService for GrpcServer<P> {
    async fn process_data(
        &self,
        request: Request<DataRequest>,
    ) -> Result<Response<DataResponse>, Status> {
        let req = request.into_inner();
        
        // Generate ID if not provided
        let id = if req.id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            req.id
        };
        
        // Create event
        let event = Event {
            id: id.clone(),
            event_type: req.data_type,
            payload: req.payload,
            timestamp: Utc::now(),
            metadata: Some(crate::models::EventMetadata {
                source: "grpc-service".to_string(),
                correlation_id: None,
                created_at: Utc::now(),
            }),
        };
        
        // Publish event
        match self.producer.publish(&event).await {
            Ok(_) => Ok(Response::new(DataResponse {
                id,
                success: true,
                message: "Data processed successfully".to_string(),
            })),
            Err(e) => {
                tracing::error!("Failed to publish event: {}", e);
                Err(Status::internal("Failed to process data"))
            }
        }
    }
    
    type StreamDataStream = mpsc::Receiver<Result<DataResponse, Status>>;
    
    async fn stream_data(
        &self,
        request: Request<tonic::Streaming<DataRequest>>,
    ) -> Result<Response<Self::StreamDataStream>, Status> {
        let mut stream = request.into_inner();
        let producer = self.producer.clone();
        
        let (tx, rx) = mpsc::channel(128);
        
        tokio::spawn(async move {
            while let Some(result) = stream.message().await {
                match result {
                    Ok(req) => {
                        // Generate ID if not provided
                        let id = if req.id.is_empty() {
                            Uuid::new_v4().to_string()
                        } else {
                            req.id
                        };
                        
                        // Create event
                        let event = Event {
                            id: id.clone(),
                            event_type: req.data_type,
                            payload: req.payload,
                            timestamp: Utc::now(),
                            metadata: Some(crate::models::EventMetadata {
                                source: "grpc-service".to_string(),
                                correlation_id: None,
                                created_at: Utc::now(),
                            }),
                        };
                        
                        // Publish event
                        let success = match producer.publish(&event).await {
                            Ok(_) => true,
                            Err(e) => {
                                tracing::error!("Failed to publish event: {}", e);
                                false
                            }
                        };
                        
                        let response = DataResponse {
                            id,
                            success,
                            message: if success {
                                "Data processed successfully".to_string()
                            } else {
                                "Failed to process data".to_string()
                            },
                        };
                        
                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                    },
                    Err(e) => {
                        if tx.send(Err(e)).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        
        Ok(Response::new(rx))
    }
}