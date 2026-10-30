//! JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ListGroups, DescribeGroups,
//! DeleteGroups: the coordinator ([`crate::groups`]) on the raft leader. Any
//! other node answers NOT_COORDINATOR, and the client asks FindCoordinator
//! again.

use std::sync::Arc;
use std::time::Instant;

use kafka_protocol::messages::delete_groups_response::DeletableGroupResult;
use kafka_protocol::messages::describe_groups_response::{DescribedGroup, DescribedGroupMember};
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::messages::list_groups_response::ListedGroup;
use kafka_protocol::messages::{
    DeleteGroupsRequest, DeleteGroupsResponse, DescribeGroupsRequest, DescribeGroupsResponse,
    GroupId, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse,
    LeaveGroupRequest, LeaveGroupResponse, ListGroupsRequest, ListGroupsResponse, SyncGroupRequest,
    SyncGroupResponse,
};

use super::s;
use crate::conn::Conn;
use crate::errors::*;
use crate::groups::{JoinArgs, JoinOut, Joined, SyncOut, Synced};
use crate::server::Ctx;

fn coordinates(ctx: &Ctx) -> bool {
    ctx.broker.cluster().leads()
}

/// A group id Queen can name: not empty, at most 255 bytes.
pub fn valid_group(g: &str) -> bool {
    !g.is_empty() && g.len() <= 255
}

pub async fn join(
    ctx: &Arc<Ctx>,
    conn: &Conn,
    req: JoinGroupRequest,
    version: i16,
) -> JoinGroupResponse {
    let member = req.member_id.to_string();
    let out = if !coordinates(ctx) {
        JoinOut::error(NOT_COORDINATOR, &member)
    } else if !valid_group(req.group_id.0.as_str()) {
        JoinOut::error(INVALID_GROUP_ID, &member)
    } else {
        let args = JoinArgs {
            group: req.group_id.0.to_string(),
            member_id: member.clone(),
            client_id: conn.client_id.clone(),
            client_host: conn.client_host.clone(),
            session_timeout_ms: req.session_timeout_ms,
            rebalance_timeout_ms: if version >= 1 {
                req.rebalance_timeout_ms
            } else {
                req.session_timeout_ms
            },
            protocol_type: req.protocol_type.to_string(),
            // Copied: the decoded metadata is a slice of the request frame,
            // and the member keeps it for as long as it is a member.
            protocols: req
                .protocols
                .into_iter()
                .map(|p| (p.name.to_string(), bytes::Bytes::copy_from_slice(&p.metadata)))
                .collect(),
            require_known_member: version >= 4,
        };
        let group = args.group.clone();
        let out = match ctx.groups.join(args, Instant::now()) {
            Joined::Now(o) => o,
            Joined::Wait(rx) => rx
                .await
                .unwrap_or_else(|_| JoinOut::error(UNKNOWN_MEMBER_ID, &member)),
        };
        ctx.groups.note_client(&group, conn.bearer.as_deref());
        out
    };
    JoinGroupResponse::default()
        .with_error_code(out.error)
        .with_generation_id(out.generation)
        .with_protocol_name(Some(s(out.protocol.unwrap_or_default())))
        .with_leader(s(out.leader))
        .with_member_id(s(out.member_id))
        .with_members(
            out.members
                .into_iter()
                .map(|(id, metadata)| {
                    JoinGroupResponseMember::default()
                        .with_member_id(s(id))
                        .with_metadata(metadata)
                })
                .collect(),
        )
}

pub async fn sync(ctx: &Arc<Ctx>, req: SyncGroupRequest) -> SyncGroupResponse {
    let out = if !coordinates(ctx) {
        SyncOut::error(NOT_COORDINATOR)
    } else {
        let assignments = req
            .assignments
            .into_iter()
            // Copied out of the frame, as a join's metadata is.
            .map(|a| {
                (
                    a.member_id.to_string(),
                    bytes::Bytes::copy_from_slice(&a.assignment),
                )
            })
            .collect();
        match ctx.groups.sync(
            req.group_id.0.as_str(),
            req.member_id.as_str(),
            req.generation_id,
            assignments,
            Instant::now(),
        ) {
            Synced::Now(o) => o,
            Synced::Wait(rx) => rx
                .await
                .unwrap_or_else(|_| SyncOut::error(UNKNOWN_MEMBER_ID)),
        }
    };
    SyncGroupResponse::default()
        .with_error_code(out.error)
        .with_assignment(out.assignment)
}

pub fn heartbeat(ctx: &Arc<Ctx>, conn: &Conn, req: HeartbeatRequest) -> HeartbeatResponse {
    let error = if !coordinates(ctx) {
        NOT_COORDINATOR
    } else {
        ctx.groups
            .note_client(req.group_id.0.as_str(), conn.bearer.as_deref());
        ctx.groups.heartbeat(
            req.group_id.0.as_str(),
            req.member_id.as_str(),
            req.generation_id,
            Instant::now(),
        )
    };
    HeartbeatResponse::default().with_error_code(error)
}

pub fn leave(ctx: &Arc<Ctx>, req: LeaveGroupRequest) -> LeaveGroupResponse {
    let error = if !coordinates(ctx) {
        NOT_COORDINATOR
    } else {
        ctx.groups.leave(
            req.group_id.0.as_str(),
            req.member_id.as_str(),
            Instant::now(),
        )
    };
    LeaveGroupResponse::default().with_error_code(error)
}

/// Every broker is asked; only the leader coordinates any group.
pub fn list(ctx: &Arc<Ctx>, req: ListGroupsRequest) -> ListGroupsResponse {
    if !coordinates(ctx) {
        return ListGroupsResponse::default();
    }
    let filter: Vec<String> = req.states_filter.iter().map(|s| s.to_string()).collect();
    let groups = ctx
        .groups
        .list()
        .into_iter()
        .filter(|(_, _, state)| filter.is_empty() || filter.iter().any(|f| f == state.name()))
        .map(|(name, protocol_type, state)| {
            ListedGroup::default()
                .with_group_id(GroupId(s(name)))
                .with_protocol_type(s(protocol_type))
                .with_group_state(s(state.name()))
        })
        .collect();
    ListGroupsResponse::default().with_groups(groups)
}

pub fn describe(ctx: &Arc<Ctx>, req: DescribeGroupsRequest) -> DescribeGroupsResponse {
    let leads = coordinates(ctx);
    // A group named twice is described once: each description copies every
    // member's metadata.
    let mut seen = std::collections::HashSet::new();
    let groups = req
        .groups
        .into_iter()
        .filter(|g| seen.insert(g.0.clone()))
        .map(|g| {
            let base = DescribedGroup::default().with_group_id(g.clone());
            if !leads {
                return base.with_error_code(NOT_COORDINATOR);
            }
            if !valid_group(g.0.as_str()) {
                return base.with_error_code(INVALID_GROUP_ID);
            }
            match ctx.groups.describe(g.0.as_str()) {
                None => base.with_group_state(s("Dead")),
                Some(v) => base
                    .with_group_state(s(v.state.name()))
                    .with_protocol_type(s(v.protocol_type))
                    .with_protocol_data(s(v.protocol))
                    .with_members(
                        v.members
                            .into_iter()
                            .map(|m| {
                                DescribedGroupMember::default()
                                    .with_member_id(s(m.id))
                                    .with_client_id(s(m.client_id))
                                    .with_client_host(s(m.client_host))
                                    .with_member_metadata(m.metadata)
                                    .with_member_assignment(m.assignment)
                            })
                            .collect(),
                    ),
            }
        })
        .collect();
    DescribeGroupsResponse::default().with_groups(groups)
}

/// A group with no members: its membership goes, and its positions with it.
pub async fn delete(ctx: &Arc<Ctx>, conn: &Conn, req: DeleteGroupsRequest) -> DeleteGroupsResponse {
    let leads = coordinates(ctx);
    let mut results = Vec::with_capacity(req.groups_names.len());
    for g in req.groups_names {
        let error = if !leads {
            NOT_COORDINATOR
        } else if !valid_group(g.0.as_str()) {
            INVALID_GROUP_ID
        } else {
            match ctx.groups.delete(g.0.as_str()) {
                Err(code) => code,
                Ok(in_memory) => match ctx
                    .queen
                    .forget_group(conn.bearer.as_deref(), g.0.as_str())
                    .await
                {
                    Ok(n) if n > 0 || in_memory => NONE,
                    Ok(_) => GROUP_ID_NOT_FOUND,
                    Err(e) => of_call_for_group(&e),
                },
            }
        };
        results.push(
            DeletableGroupResult::default()
                .with_group_id(g)
                .with_error_code(error),
        );
    }
    DeleteGroupsResponse::default().with_results(results)
}
