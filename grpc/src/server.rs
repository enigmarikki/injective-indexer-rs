use tonic::{Request, Response, Status};
use uuid::Uuid;
use std::sync::Arc;
use tokio::sync::mpsc;
use chrono::Utc;
//use crate::producer::MessageProducer;
// Here we get the data and put it in a message queue
// Why do i need mpsc - idk yet
use crate::proto::injective::stream::v1beta1::{
    StreamRequest, StreamResponse, stream_client
};


