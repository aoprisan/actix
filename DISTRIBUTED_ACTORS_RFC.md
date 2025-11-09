# RFC: Distributed Actors for Actix (actix-cluster)

**Status**: Draft
**Author**: Architecture Design
**Created**: 2025-11-09

## Summary

This RFC proposes adding distributed actor capabilities to the Actix framework through a new `actix-cluster` crate. The design enables location-transparent actor communication across multiple nodes while maintaining backward compatibility with existing local-only actor systems.

## Motivation

Actix currently supports only local actor communication within a single process or across threads via `Arbiter`. Real-world applications often require:
- Horizontal scalability across multiple machines
- Fault tolerance through actor migration and replication
- Geographic distribution of actors
- Load balancing across a cluster

This RFC addresses these needs by extending Actix's actor model to support distributed systems.

## Research: Existing Distributed Actor Frameworks

### Akka (JVM)
**Key Design Patterns:**
- Hierarchical actor paths: `akka://[email protected]:5678/user/service-b`
- Remote actor references with transparent serialization
- Peer-to-peer remoting as foundation for clustering
- Location transparency through unified addressing

**Lessons for Actix:**
- Actor paths should encode location (protocol, host, port)
- Local and remote references need unified interface
- Remoting should be foundation, clustering built on top

### Orleans (.NET)
**Key Design Patterns:**
- Virtual actors that "always exist" conceptually
- Automatic instantiation/deactivation (grains)
- Silo-based clustering with automatic placement
- No explicit actor creation/destruction
- Built-in caching and state management

**Lessons for Actix:**
- Automatic actor activation useful for stateless actors
- Placement strategies should be pluggable
- Runtime should handle lifecycle automatically

### Erlang/OTP
**Key Design Patterns:**
- Transparent remote messaging via PIDs
- Supervision trees across nodes
- Lightweight processes with isolated memory
- Network transparency but manual distributed system concerns

**Lessons for Actix:**
- Message passing should be transparent
- Supervision must extend to remote actors
- Developers need control over distributed semantics (ordering, delivery guarantees)

## Design Overview

### Architecture Diagram

```
┌────────────────────────────────────────────────────────────────┐
│                        Actix Cluster                            │
│                                                                 │
│  Node A (192.168.1.10)          Node B (192.168.1.11)          │
│  ┌──────────────────┐            ┌──────────────────┐          │
│  │  Actor System    │            │  Actor System    │          │
│  │                  │            │                  │          │
│  │  ┌────────────┐  │            │  ┌────────────┐  │          │
│  │  │ ActorA     │  │            │  │ ActorB     │  │          │
│  │  │            │  │            │  │            │  │          │
│  │  │ ActorRef───┼──┼────────────┼──┤            │  │          │
│  │  │ <ActorB>   │  │  Network   │  │            │  │          │
│  │  └────────────┘  │  Transport │  └────────────┘  │          │
│  │        │         │            │        ▲         │          │
│  │        ▼         │            │        │         │          │
│  │  ┌────────────┐  │            │  ┌─────┴──────┐  │          │
│  │  │ Router     │  │◄──────────►│  │ Router     │  │          │
│  │  └────────────┘  │            │  └────────────┘  │          │
│  │        │         │            │                  │          │
│  │        ▼         │            │                  │          │
│  │  ┌────────────┐  │            │  ┌────────────┐  │          │
│  │  │ Cluster    │  │◄──Gossip──►│  │ Cluster    │  │          │
│  │  │ Registry   │  │            │  │ Registry   │  │          │
│  │  └────────────┘  │            │  └────────────┘  │          │
│  └──────────────────┘            └──────────────────┘          │
└────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Network-Transparent Addressing

Extend Actix's addressing with location awareness:

```rust
// Current: Local only
pub struct Addr<A: Actor> { /* ... */ }

// Proposed: Unified local/remote addressing
pub enum ActorRef<A: Actor> {
    Local(Addr<A>),
    Remote(RemoteAddr<A>),
}

pub struct RemoteAddr<A: Actor> {
    node_id: NodeId,
    local_path: ActorPath,
    _phantom: PhantomData<A>,
}

pub struct NodeId {
    protocol: Protocol,  // "actix-tcp", "actix-quic"
    host: String,
    port: u16,
}

pub struct ActorPath(String);  // "/user/service-a/worker1"
```

**Design Decision**: Use enum rather than trait object for ActorRef to:
- Maintain zero-cost abstraction for local-only systems
- Enable compiler optimizations
- Provide clear type distinction

### 2. Message Serialization

Messages sent across network must be serializable:

```rust
// New trait for remote messages
pub trait RemoteMessage: Message + Serialize + DeserializeOwned {
    // Optional: Message type ID for versioning
    fn message_type_id() -> &'static str {
        std::any::type_name::<Self>()
    }
}

// Automatic implementation for local messages with serde
impl<M> RemoteMessage for M
where
    M: Message + Serialize + DeserializeOwned
{
}
```

**Design Decision**: Separate `RemoteMessage` from `Message` to:
- Maintain backward compatibility (not all messages need serialization)
- Make distributed capability explicit
- Allow future extension (versioning, schema evolution)

### 3. Transport Layer

Abstract network transport:

```rust
pub trait Transport: Send + Sync {
    fn send(&self, node: &NodeId, envelope: SerializedEnvelope)
        -> impl Future<Output = Result<(), TransportError>>;

    fn receive(&self)
        -> impl Stream<Item = (NodeId, SerializedEnvelope)>;
}

pub struct TcpTransport {
    // Connection pool per node
    connections: Arc<RwLock<HashMap<NodeId, Connection>>>,
}

// Frame format: [4 byte length][message_type_id][payload]
pub struct SerializedEnvelope {
    message_type: String,
    payload: Bytes,
}
```

**Codec Options**:
- **Bincode**: Default (compact, fast, Rust-specific)
- **MessagePack**: Alternative (language-agnostic, compact)
- **JSON**: Debug/development (human-readable, verbose)

### 4. Cluster Membership

Track cluster state using gossip protocol:

```rust
pub struct ClusterRegistry {
    // Local node information
    local_node: NodeId,

    // Known nodes with health status
    members: Arc<RwLock<HashMap<NodeId, NodeMetadata>>>,

    // Gossip protocol for membership
    gossip: GossipProtocol,
}

pub struct NodeMetadata {
    status: NodeStatus,        // Up, Down, Leaving, Joining
    version: u64,              // Logical clock for conflict resolution
    actors: HashSet<ActorPath>, // Actors hosted on this node
}

pub enum NodeStatus {
    Up,
    Down,
    Joining,
    Leaving,
}
```

**Gossip Protocol**:
- Periodic peer-to-peer state exchange (every 1-5 seconds)
- SWIM-style failure detection (Scalable Weakly-consistent Infection-style Membership)
- Configurable gossip fanout and intervals

### 5. Remote Message Routing

```rust
pub struct Router {
    cluster: Arc<ClusterRegistry>,
    transport: Arc<dyn Transport>,
    local_system: System,
}

impl Router {
    pub async fn route<A, M>(&self, target: &ActorRef<A>, msg: M)
    where
        A: Actor,
        M: RemoteMessage,
        A: Handler<M>,
    {
        match target {
            ActorRef::Local(addr) => {
                // Fast path: local delivery
                addr.send(msg).await
            }
            ActorRef::Remote(remote_addr) => {
                // Serialize message
                let envelope = self.serialize_message(msg)?;

                // Send over network
                self.transport.send(&remote_addr.node_id, envelope).await?;
            }
        }
    }
}
```

### 6. Actor Discovery and Registry

Extend `SystemRegistry` for cluster-wide lookups:

```rust
pub trait ClusterService: Actor {
    fn service_name() -> &'static str;
}

pub struct ClusterServiceRegistry {
    // Local cache of actor locations
    cache: Arc<RwLock<HashMap<String, ActorRef<dyn ClusterService>>>>,

    // Cluster membership
    cluster: Arc<ClusterRegistry>,
}

impl ClusterServiceRegistry {
    pub async fn resolve<S: ClusterService>(&self) -> Option<ActorRef<S>> {
        let name = S::service_name();

        // Check local cache
        if let Some(actor_ref) = self.cache.read().await.get(name) {
            return Some(actor_ref.clone());
        }

        // Query cluster
        self.cluster_lookup(name).await
    }
}
```

## Delivery Guarantees

Three levels of message delivery semantics:

### At-Most-Once (Default)
- Fire-and-forget
- No acknowledgment
- Lowest latency
- Use for non-critical messages

```rust
actor_ref.do_send(msg);  // Existing API, works with ActorRef
```

### At-Least-Once
- Retry until acknowledged
- May deliver duplicates
- Use for idempotent operations

```rust
actor_ref.send_reliable(msg).await?;  // New API
```

### Exactly-Once (Future)
- Deduplication + acknowledgment
- Highest overhead
- Use for critical state changes

## Fault Tolerance

### Network Partition Handling

**Strategy**: Prefer Availability over Consistency (AP in CAP)

- Detect partitions via gossip timeout
- Continue operation in partition
- Log partition events
- Reconcile state when partition heals

**Configuration**:
```rust
pub struct ClusterConfig {
    // How long without gossip before marking node down
    pub failure_detection_timeout: Duration,

    // Allow partial partition operation
    pub allow_split_brain: bool,

    // Minimum cluster size to operate
    pub min_cluster_size: usize,
}
```

### Remote Actor Supervision

Extend supervisor to monitor remote actors:

```rust
impl Supervisor {
    pub fn supervise_remote<A: Actor>(
        &self,
        remote_ref: RemoteAddr<A>,
        strategy: SupervisionStrategy,
    ) {
        // Monitor node health
        // On node failure:
        //   - Restart actor on another node (if configured)
        //   - Notify parent supervisor
        //   - Update cluster registry
    }
}

pub enum SupervisionStrategy {
    // Restart on same node when it comes back
    RestartOnSameNode,

    // Migrate to another node immediately
    MigrateToAvailableNode,

    // Custom placement logic
    Custom(Box<dyn PlacementStrategy>),
}
```

## Actor Placement Strategies

```rust
pub trait PlacementStrategy: Send + Sync {
    fn choose_node(&self, cluster: &ClusterRegistry) -> Option<NodeId>;
}

pub struct RoundRobinPlacement;
pub struct ConsistentHashPlacement;
pub struct AffinityBasedPlacement {
    // Prefer nodes with related actors
    affinity_key: String,
}
```

## Migration Path

### Phase 1: Local Development
Existing code works unchanged:
```rust
// No changes needed
let addr = MyActor.start();
addr.send(MyMessage).await?;
```

### Phase 2: Opt-in Remote Messaging
Enable distributed features:
```rust
use actix_cluster::prelude::*;

// Messages need Serialize/Deserialize
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "()")]
struct MyMessage;

// Start cluster node
let cluster = ClusterNode::new(config).await?;

// Create remote reference
let remote_addr = cluster.resolve::<MyService>().await?;
remote_addr.send(MyMessage).await?;
```

### Phase 3: Full Cluster
```rust
// Automatic actor placement
#[derive(Actor)]
#[cluster_service(name = "my-service", placement = "round-robin")]
struct MyService;

// Framework handles distribution automatically
let service = ClusterService::<MyService>::start(&cluster).await?;
```

## Implementation Phases

### Phase 1: Foundation (Weeks 1-4)
- [ ] Create `actix-cluster` crate structure
- [ ] Implement `ActorRef<A>` and `RemoteAddr<A>`
- [ ] Define `RemoteMessage` trait
- [ ] Basic TCP transport with bincode serialization
- [ ] Simple two-node cluster tests

### Phase 2: Cluster Management (Weeks 5-8)
- [ ] Implement gossip protocol (SWIM-based)
- [ ] `ClusterRegistry` with membership tracking
- [ ] Node join/leave protocol
- [ ] Failure detection and health checks
- [ ] Multi-node integration tests

### Phase 3: Routing & Discovery (Weeks 9-12)
- [ ] Message router with local/remote dispatch
- [ ] `ClusterServiceRegistry` for actor discovery
- [ ] Actor location caching
- [ ] Message delivery guarantees (at-most-once, at-least-once)

### Phase 4: Fault Tolerance (Weeks 13-16)
- [ ] Remote supervision
- [ ] Actor migration on node failure
- [ ] Network partition detection and handling
- [ ] State reconciliation

### Phase 5: Advanced Features (Weeks 17-20)
- [ ] Placement strategies (round-robin, consistent hash, affinity)
- [ ] Cluster-wide pub/sub (extend actix-broker)
- [ ] Metrics and observability
- [ ] Performance benchmarks

### Phase 6: Production Readiness (Weeks 21-24)
- [ ] Comprehensive documentation
- [ ] Migration guide
- [ ] Security: TLS support, authentication
- [ ] Production-grade logging and tracing
- [ ] Load testing and optimization

## API Design Examples

### Starting a Cluster Node

```rust
use actix_cluster::prelude::*;

#[actix::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = ClusterConfig {
        node_id: NodeId::new("actix-tcp", "192.168.1.10", 8080),
        seed_nodes: vec![
            NodeId::new("actix-tcp", "192.168.1.11", 8080),
        ],
        gossip_interval: Duration::from_secs(1),
        failure_detection_timeout: Duration::from_secs(10),
    };

    let cluster = ClusterNode::start(config).await?;

    // Register local services
    cluster.register_service::<MyService>().await?;

    // Keep node running
    cluster.run().await
}
```

### Sending Messages to Remote Actors

```rust
// Resolve service by name
let service = cluster
    .resolve_service::<CalculatorService>()
    .await?;

// Send message (works for both local and remote)
let result = service.send(Add(5, 3)).await?;
println!("Result: {}", result);
```

### Actor with Remote Messaging

```rust
use actix::prelude::*;
use serde::{Serialize, Deserialize};

// Message must be serializable for remote delivery
#[derive(Message, Serialize, Deserialize)]
#[rtype(result = "u32")]
struct Add(u32, u32);

struct CalculatorService;

impl Actor for CalculatorService {
    type Context = Context<Self>;
}

impl Handler<Add> for CalculatorService {
    type Result = u32;

    fn handle(&mut self, msg: Add, _ctx: &mut Context<Self>) -> u32 {
        msg.0 + msg.1
    }
}

// Register as cluster service
impl ClusterService for CalculatorService {
    fn service_name() -> &'static str {
        "calculator"
    }
}
```

## Open Questions

1. **Serialization Format**: Support multiple codecs or standardize on bincode?
   - **Recommendation**: Default to bincode, allow pluggable codecs

2. **Actor Lifecycle**: Should remote actors auto-activate like Orleans grains?
   - **Recommendation**: Start with explicit activation, add auto-activation as opt-in feature

3. **State Management**: Built-in distributed state or leave to user?
   - **Recommendation**: Leave to user initially, provide examples with distributed stores (Redis, etc.)

4. **Security**: TLS by default or opt-in?
   - **Recommendation**: Opt-in for Phase 1, mandatory for production

5. **Backwards Compatibility**: Keep `Addr<A>` or migrate to `ActorRef<A>`?
   - **Recommendation**: Keep `Addr<A>`, add conversion to `ActorRef<A>`

## Success Criteria

- Zero breaking changes to existing actix code
- < 5% latency overhead for local-only systems with cluster features disabled
- Support 100+ node clusters
- Sub-millisecond local message delivery
- < 10ms p99 remote message delivery on 1Gbps network
- Comprehensive documentation and examples
- Production use case validation

## References

- [Akka Remoting Documentation](https://doc.akka.io/docs/akka/current/remoting.html)
- [Orleans Documentation](https://learn.microsoft.com/en-us/dotnet/orleans/overview)
- [SWIM: Scalable Weakly-consistent Infection-style Membership Protocol](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)
- [Erlang Distribution Protocol](https://www.erlang.org/doc/apps/erts/erl_dist_protocol.html)
- [CAP Theorem and Distributed Systems](https://www.infoq.com/articles/cap-twelve-years-later-how-the-rules-have-changed/)
