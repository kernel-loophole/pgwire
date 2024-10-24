//! APIs for building PostgreSQL compatible servers.

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::sync::Arc;
use futures::Sink;
pub use postgres_types::Type;
use tokio_postgres::{Row, SimpleQueryMessage, SimpleQueryRow};
use crate::api::auth::noop::NoopStartupHandler;
use crate::api::copy::NoopCopyHandler;
use crate::api::portal::Portal;
use crate::api::query::{DataRowEncoder, PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use crate::api::results::{DescribePortalResponse, DescribeStatementResponse, FieldFormat, QueryResponse};
use crate::api::stmt::StoredStatement;
use crate::api::store::PortalStore;
use crate::error::{PgWireError, PgWireResult};
use crate::messages::PgWireBackendMessage;

pub mod auth;
pub mod copy;
pub mod portal;
pub mod query;
pub mod results;
pub mod stmt;
pub mod store;

pub const DEFAULT_NAME: &str = "POSTGRESQL_DEFAULT_NAME";

#[derive(Debug, Clone, Copy, Default)]
pub enum PgWireConnectionState {
    #[default]
    AwaitingSslRequest,
    AwaitingStartup,
    AuthenticationInProgress,
    ReadyForQuery,
    QueryInProgress,
    CopyInProgress(bool),
    AwaitingSync,
}

/// Describe a client information holder
pub trait ClientInfo {
    fn socket_addr(&self) -> SocketAddr;
    fn is_secure(&self) -> bool;
    fn state(&self) -> PgWireConnectionState;
    fn set_state(&mut self, new_state: PgWireConnectionState);
    fn metadata(&self) -> &HashMap<String, String>;
    fn metadata_mut(&mut self) -> &mut HashMap<String, String>;
}

/// Client Portal Store
pub trait ClientPortalStore {
    type PortalStore;

    fn portal_store(&self) -> &Self::PortalStore;
}

pub const METADATA_USER: &str = "user";
pub const METADATA_DATABASE: &str = "database";

#[non_exhaustive]
#[derive(Debug)]
pub struct DefaultClient<S> {
    pub socket_addr: SocketAddr,
    pub is_secure: bool,
    pub state: PgWireConnectionState,
    pub metadata: HashMap<String, String>,
    pub portal_store: store::MemPortalStore<S>,
}

impl<S> ClientInfo for DefaultClient<S> {
    fn socket_addr(&self) -> SocketAddr {
        self.socket_addr
    }

    fn is_secure(&self) -> bool {
        self.is_secure
    }

    fn state(&self) -> PgWireConnectionState {
        self.state
    }

    fn set_state(&mut self, new_state: PgWireConnectionState) {
        self.state = new_state;
    }

    fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.metadata
    }
}

impl<S> DefaultClient<S> {
    pub fn new(socket_addr: SocketAddr, is_secure: bool) -> DefaultClient<S> {
        DefaultClient {
            socket_addr,
            is_secure,
            state: PgWireConnectionState::default(),
            metadata: HashMap::new(),
            portal_store: store::MemPortalStore::new(),
        }
    }
}

impl<S> ClientPortalStore for DefaultClient<S> {
    type PortalStore = store::MemPortalStore<S>;

    fn portal_store(&self) -> &Self::PortalStore {
        &self.portal_store
    }
}

pub trait PgWireHandlerFactory {
    type StartupHandler: auth::StartupHandler;
    type SimpleQueryHandler: query::SimpleQueryHandler;
    type ExtendedQueryHandler: query::ExtendedQueryHandler;
    type CopyHandler: copy::CopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler>;
    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler>;
    fn startup_handler(&self) -> Arc<Self::StartupHandler>;
    fn copy_handler(&self) -> Arc<Self::CopyHandler>;
}

impl<T> PgWireHandlerFactory for Arc<T>
where
    T: PgWireHandlerFactory,
{
    type StartupHandler = T::StartupHandler;
    type SimpleQueryHandler = T::SimpleQueryHandler;
    type ExtendedQueryHandler = T::ExtendedQueryHandler;
    type CopyHandler = T::CopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        (**self).simple_query_handler()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        (**self).extended_query_handler()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        (**self).startup_handler()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        (**self).copy_handler()
    }
}

// Adding the StatelessMakeHandler implementation

pub struct StatelessMakeHandler<H> {
    handler: Arc<H>,
}

impl<H> StatelessMakeHandler<H> {
    pub fn new(handler: Arc<H>) -> Self {
        Self { handler }
    }

    // Implement the necessary handler methods
    pub fn make(&self) -> Arc<H> {
        Arc::clone(&self.handler)
    }
}

pub struct ProxyProcessor {
    upstream_client: tokio_postgres::Client,
}

#[async_trait::async_trait]
impl query::SimpleQueryHandler for ProxyProcessor {
    async fn do_query<'a, C>(
        &self,
        _client: &C,
        query: &'a str,
    ) -> PgWireResult<Vec<query::Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let resp_msgs = self.upstream_client
            .simple_query(query)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let mut downstream_response = Vec::new();
        let mut row_buf = Vec::new();

        for resp in resp_msgs {
            match resp {
                SimpleQueryMessage::CommandComplete(count) => {
                    if row_buf.is_empty() {
                        downstream_response.push(query::Response::Execution(
                            query::Tag::new_for_execution("", Some(count as usize)),
                        ));
                    } else {
                        let query_response = encode_simple_query_response(&row_buf);
                        downstream_response.push(query::Response::Query(query_response));
                    }
                }
                SimpleQueryMessage::Row(row) => {
                    row_buf.push(row); // Keep SimpleQueryRow as is
                }
                _ => {}
            }
        }

        Ok(downstream_response)
    }
}

// fn encode_simple_query_response(p0: &Vec<SimpleQueryRow>) -> Box<T> {
//     todo!()
// }
#[async_trait::async_trait]
impl query::SimpleQueryHandler for ProxyProcessor {
    async fn do_query<'a, C>(
        &self,
        _client: &C,
        query: &'a str,
    ) -> PgWireResult<Vec<query::Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // Use simple_query to get SimpleQueryMessages
        let resp_msgs = self.upstream_client
            .simple_query(query)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let mut downstream_response = Vec::new();
        let mut row_buf = Vec::new();

        for resp in resp_msgs {
            match resp {
                SimpleQueryMessage::CommandComplete(count) => {
                    if row_buf.is_empty() {
                        downstream_response.push(query::Response::Execution(
                            query::Tag::new_for_execution("", Some(count as usize)),
                        ));
                    } else {
                        // Here, you might want to handle converting the SimpleQueryRow to a usable format
                        let query_response = encode_simple_query_response(&row_buf);
                        downstream_response.push(query::Response::Query(query_response));
                    }
                }
                SimpleQueryMessage::Row(row) => {
                    // Store SimpleQueryRow for later processing
                    row_buf.push(row);
                }
                _ => {}
            }
        }

        Ok(downstream_response)
    }
}
fn encode_simple_query_response(rows: &[tokio_postgres::SimpleQueryRow]) -> query::QueryResponse {
    let mut encoder = query::DataRowEncoder::new(/* Your schema here */);

    for row in rows {
        // Loop through each field in the SimpleQueryRow
        for field in row {
            // Assuming you know the type of field or it can be inferred
            encoder.encode_field(field).unwrap(); // Make sure to handle the potential error
        }
    }

    query::QueryResponse::new(Arc::new(vec![]), encoder.finish())
}
// This function encodes the SimpleQueryRows into a format that your Response can handle



#[async_trait::async_trait]
impl query::ExtendedQueryHandler for ProxyProcessor {
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
        portal: &'a query::Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<query::Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = &portal.statement.statement;
        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![]; // Extract params from the portal
        let rows = self.upstream_client.query(query, &params).await.map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let query_response = encode_query_response(&rows);
        Ok(query::Response::Query(query_response))
    }
}
impl PgWireHandlerFactory for NoopStartupHandler {
    type StartupHandler = NoopStartupHandler;
    type SimpleQueryHandler = Arc<dyn SimpleQueryHandler + Send + Sync>;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        todo!()
    }

    // fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
    //     // Arc::new(self.clone())
    // }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(NoopStartupHandler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}
fn encode_query_response(rows: &Vec<Row>) -> QueryResponse {
    let schema = vec![
        // Define the schema according to your data structure
        ("column1", FieldFormat::Text),
        ("column2", FieldFormat::Text),
        // Add more columns if needed
    ];

    let mut encoder = DataRowEncoder::new();

    for row in rows {
        for (index, (_col_name, _format)) in schema.iter().enumerate() {
            // Access row data using the index, and encode each field.
            // Assuming fields are of type String, adjust the parsing based on actual types.
            if let Some(value) = row.get(index) {
                encoder.encode_field(&value).unwrap();
            } else {
                // Handle NULL values
                encoder.encode_field(&None::<String>).unwrap();
            }
        }
    }

    // Construct the QueryResponse from the encoded rows
    let rows_encoded = encoder.finish();
    QueryResponse::new(schema, rows_encoded)
}
pub(crate) struct DataRowEncoder();

impl DataRowEncoder {
    pub(crate) fn new() -> Box<T> {
        todo!()
    }
}
