use std::{future::Future, sync::Arc};

use crate::{conshash::ConsistentHashing, rpc::{RPCError, DEFAULT_CLIENT_POOL}};
use futures::stream::FuturesUnordered;
use tokio_stream::StreamExt;

use super::{RPCClient, ServiceClientWithId};

pub async fn broadcast_to_members<C, F, R, Fut>(
    conshash: &Arc<ConsistentHashing>,
    func: F,
) -> Result<Vec<(u64, Result<R, RPCError>)>, String>
where
    C: ServiceClientWithId,
    F: Fn(Arc<C>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<R, RPCError>> + Send,
{
    let (members, _) = match conshash.membership().all_members(true).await {
        Ok(members) => members,
        Err(e) => {
            return Err(format!("Failed to get all members: {:?}", e));
        }
    };
    let server_ids = members.into_iter().map(|m| m.id);
    broadcast_with_server_ids(server_ids, &conshash, func).await
}

pub async fn broadcast_with_server_ids<C, F, R, I, Fut>(
    server_ids: I,
    conshash: &Arc<ConsistentHashing>,
    func: F,
) -> Result<Vec<(u64, Result<R, RPCError>)>, String>
where
    I: Iterator<Item = u64>,
    C: ServiceClientWithId,
    F: Fn(Arc<C>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<R, RPCError>> + Send,
{
    let member_futs: FuturesUnordered<_> = server_ids
        .map(|sid| {
            let func = func.clone();
            async move {
                let client = match client_by_server_id(&conshash, sid).await {
                    Ok(client) => client,
                    Err(e) => {
                        error!("Failed to get client by server id {}: {:?}", sid, e);
                        return (sid, Err(e));
                    }
                };
                return (sid, func(client).await);
            }
        })
        .collect();
    let results = member_futs.collect::<Vec<_>>().await;
    Ok(results)
}

pub async fn client_by_server_id<C>(
    conshash: &Arc<ConsistentHashing>,
    server_id: u64,
) -> Result<Arc<C>, RPCError>
where
    C: ServiceClientWithId,
{
    DEFAULT_CLIENT_POOL
        .get_by_id(server_id, move |sid| conshash.to_server_name(sid))
        .await
        .map_err(|e| RPCError::IOError(e))
        .map(|c| client_by_rpc_client(&c))
}

pub fn client_by_rpc_client<C>(client: &Arc<RPCClient>) -> Arc<C>
where
    C: ServiceClientWithId,
{
    C::new(C::SERVICE_ID, client)
}
