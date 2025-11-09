# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Actix is an actor framework for Rust built on top of Tokio. It provides async/sync actors that communicate exclusively through typed messages. The repository is organized as a Cargo workspace with three crates:

- **actix**: Core actor framework
- **actix-broker**: Message broker for the actor framework
- **actix-derive**: Procedural macros for deriving `Message` and providing `#[actix::main]` and `#[actix::test]`

MSRV: Rust 1.46.0

## Development Commands

### Building
```bash
# Check workspace with default features
cargo check --workspace --bins --examples --tests

# Check with minimal features (no defaults)
cargo hack --clean-per-run check --workspace --no-default-features --tests
```

### Testing
```bash
# Run all tests with all features
cargo test -v --workspace --all-features --no-fail-fast -- --nocapture

# Run tests for specific crate
cargo test -p actix
cargo test -p actix-broker
cargo test -p actix_derive
```

### Linting
```bash
# Format check
cargo fmt --all -- --check

# Clippy
cargo clippy --workspace --tests --all-features
```

### Examples
```bash
# Run examples (from actix/ subdirectory)
cargo run --example fibonacci
cargo run --example ping
cargo run --example ring
cargo run --example weak_addr
cargo run --example weak_recipient
```

## Architecture

### Visual Overview

**Actor Lifecycle State Machine:**
```
                    ┌─────────────┐
                    │   Started   │
                    └──────┬──────┘
                           │ started() called
                           ▼
    ┌──────────────────────────────────────┐
    │            Running                    │◄──┐
    │  (processing messages)                │   │
    └──────┬───────────────────────────┬───┘   │
           │                           │       │
           │ • Context::stop()         │       │ • Create new Addr
           │ • All Addr dropped        │       │ • Add evented objects
           │ • No evented objects      │       │   (futures/streams)
           │                           │       │
           ▼                           │       │
    ┌──────────────┐                  │       │
    │   Stopping   │──────────────────┘       │
    │              │──────────────────────────┘
    └──────┬───────┘  stopping() → Running::Continue
           │
           │ stopping() → Running::Stop
           ▼
    ┌──────────────┐
    │   Stopped    │ (actor dropped)
    └──────────────┘
```

**Message Flow Architecture:**
```
  ┌─────────────┐                    ┌─────────────┐
  │  Actor A    │                    │  Actor B    │
  │             │                    │             │
  │  impl       │                    │  impl       │
  │  Handler<M> │                    │  Actor      │
  └──────┬──────┘                    └──────▲──────┘
         │                                  │
         │ 1. Get address                   │
         │    let addr = ActorB.start()     │
         │                                  │
         │ 2. Send message                  │
         │    addr.send(msg)                │
         │         │                        │
         │         ▼                        │
         │    ┌─────────────────┐           │
         │    │  Addr<ActorB>   │           │
         │    │  or             │           │
         │    │  Recipient<M>   │           │
         │    └────────┬────────┘           │
         │             │                    │
         │             ▼                    │
         │    ┌─────────────────┐           │
         │    │    Mailbox      │           │
         │    │  (crossbeam     │           │
         │    │   channel)      │           │
         │    └────────┬────────┘           │
         │             │                    │
         │             ▼                    │
         │    ┌─────────────────┐           │
         │    │   Context<B>    │───────────┘
         │    │   polls mailbox │
         │    └────────┬────────┘
         │             │
         │             ▼
         │    ┌─────────────────┐
         └────│  Handler::handle│
              │  (msg, ctx)     │
              └─────────────────┘
                     │
                     ▼
              Return Response
```

**Component Hierarchy:**
```
┌─────────────────────────────────────────────────────────┐
│                      System                              │
│                   (Tokio runtime)                        │
│                                                          │
│  ┌────────────────────────────────────────────────────┐ │
│  │              Arbiter (thread + event loop)         │ │
│  │                                                    │ │
│  │  ┌──────────────────────────────────────────────┐ │ │
│  │  │         Actor Instance                       │ │ │
│  │  │  ┌────────────────────────────────────────┐  │ │ │
│  │  │  │      Context<Actor>                    │  │ │ │
│  │  │  │                                        │  │ │ │
│  │  │  │  • Mailbox                             │  │ │ │
│  │  │  │  • Spawned Futures/Streams             │  │ │ │
│  │  │  │  • Lifecycle management                │  │ │ │
│  │  │  │  • Scheduled tasks                     │  │ │ │
│  │  │  └────────────────────────────────────────┘  │ │ │
│  │  │                                              │ │ │
│  │  │  Addressed via: Addr<Actor> / WeakAddr<A>   │ │ │
│  │  └──────────────────────────────────────────────┘ │ │
│  │                                                    │ │
│  │  (Multiple actors per arbiter)                    │ │
│  └────────────────────────────────────────────────────┘ │
│                                                          │
│  (System can have multiple arbiters)                    │
└──────────────────────────────────────────────────────────┘

Registries:
  • SystemRegistry: Global, shared across all arbiters
  • Registry: Per-arbiter, thread-local services
```

### Actor Model Fundamentals

Actix implements the actor model where:
- **Actors** encapsulate state and behavior, running in isolated execution contexts
- **Messages** are the only means of communication (all statically typed, no `Any`)
- **Addresses** (`Addr<A>`) are used to reference actors (not direct references)
- **Handlers** implement message processing via `Handler<M>` trait

### Core Components

**Actor Lifecycle** (actix/src/actor.rs):
1. **Started**: `Actor::started()` is called
2. **Running**: Actor processes messages
3. **Stopping**: Triggered by `Context::stop()`, all addresses dropped, or no evented objects; can transition back to Running
4. **Stopped**: Final state, actor is dropped

**Execution Contexts** (actix/src/context.rs, contextimpl.rs):
- `Context<A>`: Standard execution context for actors
- `SyncContext`: For synchronous actors running in thread pools
- Contexts manage the actor's mailbox, spawned futures/streams, and lifecycle

**Addressing** (actix/src/address/):
- `Addr<A>`: Strong typed address to a specific actor type
- `Recipient<M>`: Type-erased address that can only receive a specific message type (useful for storing heterogeneous actor addresses)
- `WeakAddr<A>` / `WeakRecipient<M>`: Weak references that don't prevent actor from stopping
- Message sending: `send()` (awaitable response) vs `do_send()` (fire-and-forget)

**Message Handling** (actix/src/handler.rs):
- Messages must implement `Message` trait with associated `Result` type
- Actors implement `Handler<M>` for each message type they handle
- Responses can be immediate values, futures, or actor futures

**Concurrency** (actix/src/sync.rs, actix-rt):
- `System`: The actix runtime (wraps Tokio)
- `Arbiter`: Manages a thread with its own event loop for running actors
- `SyncArbiter`: Thread pool for CPU-bound synchronous actors

**Futures Integration** (actix/src/fut/):
- `ActorFuture`: Futures that have access to actor's context during execution
- `WrapFuture` / `WrapStream`: Convert standard futures/streams to actor futures/streams
- `ActorFutureExt` / `ActorStreamExt`: Extension traits with actor-aware combinators

**Actor Registry** (actix/src/registry.rs):
- `SystemRegistry`: Global singleton registry for system-wide services
- `Registry`: Per-arbiter registry for thread-local services
- `SystemService` / `ArbiterService`: Traits for actors that are automatically registered

**Stream Handling** (actix/src/stream.rs):
- `StreamHandler<I>`: Trait for actors that process streams of items
- Integrated with actor lifecycle (streams are evented objects)

**Supervision** (actix/src/supervisor.rs):
- `Supervisor`: Actor that monitors and restarts supervised actors on failure

### Special Actors

**Built-in Actors** (actix/src/actors/):
- `mocker`: Testing utilities for mocking actors
- `resolver`: DNS resolver actor (requires `resolver` feature flag)

### actix-broker

Message broker pattern implementation allowing:
- Publish/subscribe messaging between actors
- Actors subscribe to specific message types
- Messages are broadcast to all subscribers
- Useful for event-driven architectures where actors need to react to system-wide events

Key files:
- actix-broker/src/broker.rs: Core broker actor
- actix-broker/src/subscribe.rs: Subscription management

## Feature Flags

- `macros` (default): Enables derive macros and `#[actix::main]` attribute
- `resolver`: DNS resolver actor using trust-dns
- `mailbox_assert`: Adds assertions to prevent processing too many messages on event loop (debugging)

## Key Implementation Details

**Mailbox** (actix/src/mailbox.rs, address/channel.rs):
- Default capacity: Check `DEFAULT_CAPACITY` constant
- Uses crossbeam-channel internally for message passing
- Backpressure: `send()` can fail with `SendError::Full` if mailbox is full

**IO** (actix/src/io.rs):
- `FramedWrite` / `FramedRead`: Integration with tokio-util codecs
- Allows actors to handle framed I/O (e.g., line-delimited protocols)

## Common Patterns

When implementing new features:
1. Define message types with `#[derive(Message)]` and `#[rtype(result = "...")]`
2. Implement `Actor` trait with appropriate `Context` type
3. Implement `Handler<YourMessage>` for message processing
4. Use `ctx.run_later()` / `ctx.run_interval()` for scheduled tasks (from utils.rs)
5. Spawn futures with actor context using `WrapFuture` trait
6. For actor-to-actor communication, use `Addr::send()` or `Recipient::do_send()`

When debugging:
- Actor lifecycle transitions are logged (check `log` crate usage)
- Use `mailbox_assert` feature to catch event loop saturation
- Weak addresses help identify if keeping actors alive unintentionally
