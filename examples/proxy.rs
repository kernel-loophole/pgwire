use std::fmt::Debug;
use std::sync::Arc;
use futures::stream::{self, StreamExt};
use async_trait::async_trait;
use futures::Sink;
use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls, Row, SimpleQueryMessage};
use tokio_postgres::SimpleQueryRow;
use pgwire::api::results::{FieldFormat};

use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, ClientPortalStore, StatelessMakeHandler, Type};
use pgwire::api::portal::Portal;
use pgwire::api::stmt::StoredStatement;
use pgwire::api::store::PortalStore;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;

pub struct ProxyProcessor {
    upstream_client: Client,
}

#[async_trait]
impl SimpleQueryHandler for ProxyProcessor {
    async fn do_query<'a, C>(&self, _client: &C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        self.upstream_client
            .simple_query(query)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))
            .map(|resp_msgs| {
                let mut downstream_response = Vec::new();
                let mut row_buf = Vec::new();
                for resp in resp_msgs {
                    match resp {
                        SimpleQueryMessage::CommandComplete(count) => {
                            if row_buf.is_empty() {
                                downstream_response.push(Response::Execution(
                                    Tag::new_for_execution("", Some(count as usize)),
                                ));
                            } else {
                                // Convert buffered rows to QueryResponse
                                let query_response = encode_query_response(&row_buf);
                                downstream_response.push(Response::Query(query_response));
                            }
                        }
                        SimpleQueryMessage::Row(row) => {
                            // Buffer the row for later processing
                            // row_buf.push(&row);
                        }
                        _ => {}
                    }
                }
                downstream_response
            })
    }
}

fn encode_query_response(rows: &Vec<Row>) -> QueryResponse {
    // Define the schema according to your data structure
    let schema = vec![
        ("column1", FieldFormat::Text),
        ("column2", FieldFormat::Text),
        // Add more columns if needed based on your actual schema
    ];

    let mut encoded_rows = Vec::new(); // This will store encoded DataRow objects

    // Iterate over each row to encode it
    for row in rows {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for (index, (_col_name, _format)) in schema.iter().enumerate() {
            // Access the row data using the index, and encode each field.
            // Assuming fields are of type String, adjust the parsing based on actual types.
            if let Some(value) = row.get::<_, String>(index) {
                encoder.encode_field(&value).unwrap();
            } else {
                // Handle NULL values
                encoder.encode_field(&None::<String>).unwrap();
            }
        }
        // Finish encoding the row and add it to the encoded rows vector
        encoded_rows.push(Ok(encoder.finish()));
    }

    // Convert the vector into a stream
    let rows_stream = stream::iter(encoded_rows);

    // Construct the QueryResponse from the schema and the stream of rows
    QueryResponse::new(schema, rows_stream)
}

#[async_trait]
impl ExtendedQueryHandler for ProxyProcessor {
    type Statement = String;
    type QueryParser = ();

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        todo!()
    }

    async fn do_describe_statement<C>(&self, client: &mut C, target: &StoredStatement<Self::Statement>) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement=Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>
    {
        todo!()
    }

    async fn do_describe_portal<C>(&self, client: &mut C, target: &Portal<Self::Statement>) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement=Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>
    {
        todo!()
    }

    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &'a pgwire::api::portal::Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // Implement the logic for handling extended queries
        let query = &portal.statement.statement;
        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![]; // Extract params from the portal
        let rows = self.upstream_client.query(query, &params).await.map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let query_response = encode_query_response(&rows);
        Ok(Response::Query(query_response))
    }
}

#[tokio::main]
pub async fn main() {
    let (client, connection) = tokio_postgres::connect("host=127.0.0.1 user=postgres", NoTls)
        .await
        .expect("Cannot client upstream connection");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Upstream connection error: {}", e);
        }
    });

    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(ProxyProcessor {
        upstream_client: client,
    })));

    // We have not implemented extended query in this server, use placeholder instead
    let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
        PlaceholderExtendedQueryHandler,
    )));
    let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));

    let server_addr = "127.0.0.1:5431";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let (incoming_socket, _) = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        let placeholder_ref = placeholder.make();
        tokio::spawn(async move {
            process_socket(incoming_socket, None, authenticator_ref).await;
        });
    }
}
