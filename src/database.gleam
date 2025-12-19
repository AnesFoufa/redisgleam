import command
import file_streams/file_stream
import filepath
import gleam/bit_array
import gleam/dict
import gleam/erlang/atom
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option}
import gleam/order
import gleam/otp/actor
import gleam/result
import gleam/time/duration
import gleam/time/timestamp
import rdb.{type Item}
import redis_ets as ets
import resp.{type Resp}

const default_repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

const empty_rdb_hex = 0x524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2

// Batch size for replication (send after this many writes)
const replication_batch_size = 100

// Max delay before flushing replication queue (milliseconds)
const replication_max_delay_ms = 5

pub opaque type Database {
  Database(inner: Subject(Message), config: Config, ets_table: atom.Atom)
}

pub type Config {
  Config(
    dir: String,
    db_filename: String,
    port: Int,
    replicaof: Option(#(String, Int)),
  )
}

pub fn start(config: Config) -> Database {
  let replication_state = case config.replicaof {
    option.Some(#(master_host, master_port)) -> Slave(master_host, master_port)
    option.None -> master()
  }

  // Create ETS table for fast concurrent reads
  let ets_table = ets.new("redis_data")

  let replication_queue = ReplicationQueue([], timestamp.system_time())
  let state = DatabaseState(ets_table, replication_state, replication_queue)
  let assert Ok(subject) = actor.start(state, message_handler)
  read_data_from_file(subject, config)
  start_flush_timer(subject)
  Database(inner: subject, config:, ets_table:)
}

pub fn handle_command(db: Database, cmd: command.Command) -> Resp {
  case cmd {
    command.Ping -> resp.SimpleString(<<"PONG">>)
    command.Echo(value) -> value
    command.Get(key) -> get(db, key)
    command.Set(key, value, duration) -> set(db, key, value, duration)
    command.GetConfigDbFileName ->
      resp.Array([
        resp.BulkString(<<"db_filename">>),
        resp.BulkString(bit_array.from_string(db.config.db_filename)),
      ])
    command.GetConfigDir ->
      resp.Array([
        resp.BulkString(<<"dir">>),
        resp.BulkString(bit_array.from_string(db.config.dir)),
      ])
    command.InfoReplication -> {
      case db.config.replicaof {
        option.Some(_) -> resp.BulkString(bit_array.from_string("role:slave"))
        option.None ->
          resp.BulkString(bit_array.from_string(
            "role:master\r\nmaster_replid:"
            <> default_repl_id
            <> "\r\nmaster_repl_offset:0",
          ))
      }
    }
    command.ReplConf(_replconf_cmd) -> resp.SimpleString(<<"OK">>)
    command.Psync -> psync(db)
    command.Keys -> keys(db)
    command.Wait(numreplicas, timeout) -> wait(db, numreplicas, timeout)
  }
}

fn wait(db: Database, numreplicas: Int, timeout: Int) -> Resp {
  // Start the WAIT request
  let response_channel = process.new_subject()
  let response =
    process.call_forever(
      db.inner,
      message(StartWait(numreplicas, timeout, response_channel:)),
    )

  case response {
    // If master offset is 0, we got the final count immediately
    resp.Integer(_) -> response
    // Otherwise, poll for completion
    _ -> wait_loop(db, numreplicas, timeout, 0)
  }
}

fn wait_loop(db: Database, numreplicas: Int, timeout: Int, elapsed: Int) -> Resp {
  // Check if timeout expired
  case elapsed >= timeout {
    True -> {
      // Timeout - get current count regardless of threshold
      process.call_forever(db.inner, message(GetWaitCount))
    }
    False -> {
      // Check current progress
      let result =
        process.call_forever(db.inner, message(CheckWaitProgress(numreplicas)))
      case result {
        resp.Integer(_) -> result
        // Still waiting
        _ -> {
          // Sleep 10ms and check again
          process.sleep(10)
          wait_loop(db, numreplicas, timeout, elapsed + 10)
        }
      }
    }
  }
}

/// Apply a command silently (no response). Used for processing propagated commands from master.
/// Writes directly to ETS for maximum performance on replicas.
pub fn apply_command_silent(db: Database, cmd: command.Command) -> Nil {
  case cmd {
    command.Set(key, value, duration) -> {
      // Calculate expiration
      let expires_at =
        duration
        |> option.map(fn(d) {
          timestamp.add(timestamp.system_time(), duration.milliseconds(d))
        })
      let item = rdb.Item(value, expires_at)

      // Write directly to ETS (no actor, no replication queue)
      ets.insert(db.ets_table, key, item)
      Nil
    }
    // Other write commands can be added here as needed
    _ -> Nil
  }
}

pub fn register(db: Database, subject) {
  process.call_forever(db.inner, message(Register(subject)))
}

pub fn handle_ack_from_replica(
  db: Database,
  pid: process.Pid,
  offset: Int,
) -> Resp {
  process.call_forever(db.inner, message(HandleAck(pid, offset)))
}

// FAST PATH: Read directly from ETS (no actor bottleneck!)
fn get(db: Database, key: BitArray) -> resp.Resp {
  case ets.lookup(db.ets_table, key) {
    option.Some(item) -> {
      // Check if expired
      case item.expires_at {
        option.Some(expires_at) -> {
          case timestamp.compare(timestamp.system_time(), expires_at) {
            order.Gt -> {
              // Expired - async cleanup (don't block read!)
              process.send(db.inner, AsyncMessage(AsyncCleanup(key)))
              resp.Null
            }
            _ -> item.value
          }
        }
        option.None -> item.value
      }
    }
    option.None -> resp.Null
  }
}

fn set(
  db: Database,
  key: BitArray,
  value: resp.Resp,
  duration_ms: option.Option(Int),
) -> resp.Resp {
  let duration_ms_validated = duration_ms |> option.map(fn(x) { int.max(x, 0) })
  let duration =
    duration_ms_validated
    |> option.map(duration.milliseconds)
  process.call_forever(
    db.inner,
    message(Set(key, value, duration, duration_ms_validated)),
  )
}

fn keys(db: Database) -> resp.Resp {
  process.call_forever(db.inner, message(Keys))
}

fn psync(db: Database) {
  process.call_forever(db.inner, message(Psync))
}

type WaitState {
  WaitState(
    target_offset: Int,
    numreplicas: Int,
    acks_received: dict.Dict(process.Pid, Int),
    wait_start: timestamp.Timestamp,
    response_channel: process.Subject(Int),
  )
}

type ReplicationState {
  Master(
    subjects_registry: dict.Dict(process.Pid, process.Subject(BitArray)),
    slaves: List(process.Subject(BitArray)),
    master_offset: Int,
    wait_state: option.Option(WaitState),
  )
  Slave(master_host: String, master_port: Int)
}

fn master() -> ReplicationState {
  Master(dict.new(), [], 0, option.None)
}

type ReplicationQueue {
  ReplicationQueue(queue: List(BitArray), last_flush: timestamp.Timestamp)
}

type DatabaseState {
  DatabaseState(
    ets_table: atom.Atom,
    replication_state: ReplicationState,
    replication_queue: ReplicationQueue,
  )
}

fn read_data_from_file(subject: Subject(Message), config: Config) {
  process.start(
    fn() {
      let path = filepath.join(config.dir, config.db_filename)
      case load_rdb_data(path) {
        Ok(data) -> {
          let _ = process.call_forever(subject, message(UpdateData(data)))
          Nil
        }
        Error(_) -> Nil
      }
    },
    False,
  )
}

fn load_rdb_data(path: String) -> Result(dict.Dict(BitArray, Item), Nil) {
  use stream <- result.try(
    file_stream.open_read(path) |> result.replace_error(Nil),
  )
  use content <- result.try(
    file_stream.read_remaining_bytes(stream) |> result.replace_error(Nil),
  )
  use rdb_data <- result.try(
    rdb.from_bit_array(content) |> result.replace_error(Nil),
  )
  rdb_data.databases |> list.first
}

type Message {
  Message(command: Command, sender: Subject(resp.Resp))
  AsyncMessage(command: AsyncCommand)
}

fn message(command: Command) {
  let res = fn(reply_with: Subject(resp.Resp)) {
    Message(command: command, sender: reply_with)
  }
  res
}

type Command {
  Set(
    key: BitArray,
    value: resp.Resp,
    duration: option.Option(duration.Duration),
    duration_ms: option.Option(Int),
  )
  Keys
  UpdateData(data: dict.Dict(BitArray, Item))
  Register(subject: process.Subject(BitArray))
  Psync
  HandleAck(pid: process.Pid, offset: Int)
  StartWait(
    numreplicas: Int,
    timeout: Int,
    response_channel: process.Subject(Int),
  )
  CheckWaitProgress(numreplicas: Int)
  GetWaitCount
}

type AsyncCommand {
  AsyncCleanup(key: BitArray)
  FlushReplication
}

fn message_handler(message: Message, state: DatabaseState) {
  case message {
    // Synchronous commands with response
    Message(command, sender) -> {
      let #(response, state) = case command {
        Set(key, value, duration, duration_ms) ->
          handle_set(key, value, duration, duration_ms, state)
        Keys -> handle_keys(state)
        UpdateData(incoming_data) -> handle_update_data(incoming_data, state)
        Register(subject) -> handle_register(subject, state)
        Psync -> handle_psync(sender, state)
        HandleAck(pid, offset) -> handle_ack(pid, offset, state)
        StartWait(numreplicas, timeout, response_channel) ->
          handle_start_wait(numreplicas, timeout, state, response_channel)
        CheckWaitProgress(numreplicas) ->
          handle_check_wait_progress(numreplicas, state)
        GetWaitCount -> handle_get_wait_count(state)
      }
      process.send(sender, response)

      // Post-processing
      case command {
        Psync -> send_rdb_to_slave(state, sender)
        Set(_, _, _, _) -> Nil
        // Async replication handled in handle_set
        _ -> Nil
      }

      actor.continue(state)
    }

    // Asynchronous commands (no response needed)
    AsyncMessage(command) -> {
      let state = case command {
        AsyncCleanup(key) -> {
          ets.delete(state.ets_table, key)
          state
        }
        FlushReplication -> flush_replication_queue(state)
      }
      actor.continue(state)
    }
  }
}

fn handle_set(
  key: BitArray,
  value: resp.Resp,
  duration: option.Option(duration.Duration),
  duration_ms: option.Option(Int),
  state: DatabaseState,
) -> #(resp.Resp, DatabaseState) {
  let expires_at =
    duration |> option.map(fn(d) { timestamp.add(timestamp.system_time(), d) })
  let item = rdb.Item(value, expires_at)

  // Write to ETS immediately (fast!)
  ets.insert(state.ets_table, key, item)

  // Queue for async replication
  let state = queue_for_replication(key, value, duration_ms, state)

  let response = resp.SimpleString(<<"OK">>)
  #(response, state)
}

fn handle_keys(state: DatabaseState) -> #(resp.Resp, DatabaseState) {
  let response =
    ets.all_keys(state.ets_table) |> list.map(resp.BulkString) |> resp.Array
  #(response, state)
}

fn handle_update_data(
  incoming_data: dict.Dict(BitArray, Item),
  state: DatabaseState,
) -> #(resp.Resp, DatabaseState) {
  let response = resp.SimpleString(<<"OK">>)
  // Load all data into ETS
  dict.to_list(incoming_data)
  |> list.each(fn(entry) {
    let #(key, item) = entry
    ets.insert(state.ets_table, key, item)
  })
  #(response, state)
}

fn handle_register(
  subject: process.Subject(BitArray),
  state: DatabaseState,
) -> #(resp.Resp, DatabaseState) {
  let response = resp.SimpleString(<<"Ok">>)
  case state.replication_state {
    Master(_, _, _, _) as m -> {
      let owner = process.subject_owner(subject)
      let subjects_registry = dict.insert(m.subjects_registry, owner, subject)
      let replication_state = Master(..m, subjects_registry:)
      let state = DatabaseState(..state, replication_state:)
      #(response, state)
    }
    Slave(_, _) -> #(response, state)
  }
}

fn handle_psync(
  sender: Subject(resp.Resp),
  state: DatabaseState,
) -> #(resp.Resp, DatabaseState) {
  let replication_state = case state.replication_state {
    Master(subjects_registry, slaves, master_offset, wait_state) -> {
      let pid = process.subject_owner(sender)
      let new_slaves = case dict.get(subjects_registry, pid) {
        Ok(subject) -> [subject, ..slaves]
        Error(_) -> slaves
      }
      Master(
        subjects_registry:,
        slaves: new_slaves,
        master_offset:,
        wait_state:,
      )
    }
    Slave(master_host, master_port) -> Slave(master_host, master_port)
  }
  let state = DatabaseState(..state, replication_state:)
  let response =
    resp.SimpleString(bit_array.from_string(
      "FULLRESYNC " <> default_repl_id <> " 0",
    ))
  #(response, state)
}

// Queue a write for async replication
fn queue_for_replication(
  key: BitArray,
  value: resp.Resp,
  duration_ms: option.Option(Int),
  state: DatabaseState,
) -> DatabaseState {
  case command_to_propagation_payload_internal(key, value, duration_ms) {
    option.Some(payload) -> {
      let queue = [payload, ..state.replication_queue.queue]
      let replication_queue =
        ReplicationQueue(..state.replication_queue, queue:)
      let state = DatabaseState(..state, replication_queue:)

      // Check if we should flush immediately
      case list.length(queue) >= replication_batch_size {
        True -> flush_replication_queue(state)
        False -> state
      }
    }
    option.None -> state
  }
}

fn handle_ack(
  pid: process.Pid,
  offset: Int,
  state: DatabaseState,
) -> #(resp.Resp, DatabaseState) {
  case state.replication_state {
    Master(registry, slaves, master_offset, wait_state) -> {
      case wait_state {
        option.Some(wait) -> {
          // Update ACKs received
          let new_acks = dict.insert(wait.acks_received, pid, offset)
          let new_wait = WaitState(..wait, acks_received: new_acks)
          let new_repl =
            Master(registry, slaves, master_offset, option.Some(new_wait))
          let new_state = DatabaseState(..state, replication_state: new_repl)
          #(resp.SimpleString(<<"OK">>), new_state)
        }
        option.None -> {
          // No active WAIT, just acknowledge
          #(resp.SimpleString(<<"OK">>), state)
        }
      }
    }
    Slave(_, _) -> #(resp.SimpleString(<<"OK">>), state)
  }
}

fn handle_start_wait(
  numreplicas: Int,
  _timeout: Int,
  state: DatabaseState,
  response_channel: process.Subject(Int),
) -> #(resp.Resp, DatabaseState) {
  // Flush any pending replication commands first
  let state = flush_replication_queue(state)

  case state.replication_state {
    Master(registry, slaves, master_offset, _) -> {
      // Check if any bytes have been sent
      case master_offset {
        0 -> {
          // No writes yet, all replicas are in sync
          let count = list.length(slaves)
          #(resp.Integer(count), state)
        }
        _ -> {
          // Send GETACK to all replicas
          send_getack_to_slaves(slaves)

          // Initialize wait state
          let wait_state =
            WaitState(
              target_offset: master_offset,
              numreplicas:,
              acks_received: dict.new(),
              wait_start: timestamp.system_time(),
              response_channel:,
            )
          let replication_state =
            Master(registry, slaves, master_offset, option.Some(wait_state))
          let new_state = DatabaseState(..state, replication_state:)

          // Return a placeholder - the actual response comes from CheckWaitProgress
          #(resp.SimpleString(<<"WAITING">>), new_state)
        }
      }
    }
    Slave(_, _) -> #(resp.Integer(0), state)
  }
}

fn handle_check_wait_progress(
  numreplicas: Int,
  state: DatabaseState,
) -> #(resp.Resp, DatabaseState) {
  case state.replication_state {
    Master(registry, slaves, master_offset, wait_state) -> {
      case wait_state {
        option.Some(wait) -> {
          let in_sync_count = count_in_sync_replicas(wait)
          case in_sync_count >= numreplicas {
            True -> {
              // Enough replicas acknowledged - clear wait state
              let new_repl =
                Master(registry, slaves, master_offset, option.None)
              let new_state =
                DatabaseState(..state, replication_state: new_repl)
              #(resp.Integer(in_sync_count), new_state)
            }
            False -> {
              // Still waiting
              #(resp.SimpleString(<<"IN_PROGRESS">>), state)
            }
          }
        }
        option.None -> {
          // No active wait - shouldn't happen, but return 0
          #(resp.Integer(0), state)
        }
      }
    }
    Slave(_, _) -> #(resp.Integer(0), state)
  }
}

fn handle_get_wait_count(state: DatabaseState) -> #(resp.Resp, DatabaseState) {
  // Return current count and clear wait_state
  case state.replication_state {
    Master(registry, slaves, master_offset, wait_state) -> {
      case wait_state {
        option.Some(wait) -> {
          let count = count_in_sync_replicas(wait)
          // Clear wait_state since we're returning the final count
          let new_repl = Master(registry, slaves, master_offset, option.None)
          let new_state = DatabaseState(..state, replication_state: new_repl)
          #(resp.Integer(count), new_state)
        }
        option.None -> {
          #(resp.Integer(0), state)
        }
      }
    }
    Slave(_, _) -> #(resp.Integer(0), state)
  }
}

fn count_in_sync_replicas(wait: WaitState) -> Int {
  dict.fold(wait.acks_received, 0, fn(count, _pid, offset) {
    case offset >= wait.target_offset {
      True -> count + 1
      False -> count
    }
  })
}

// Flush the replication queue to all slaves
fn flush_replication_queue(state: DatabaseState) -> DatabaseState {
  case state.replication_queue.queue {
    [] -> state
    // Nothing to flush
    queue -> {
      // Reverse to get correct order (we prepended)
      let commands = list.reverse(queue)
      let combined = bit_array.concat(commands)
      let bytes_sent = bit_array.byte_size(combined)

      // Send to all slaves and update offset
      let new_replication_state = case state.replication_state {
        Master(registry, slaves, offset, wait) -> {
          send_payload_to_slaves(slaves, combined)
          // Update master offset
          Master(registry, slaves, offset + bytes_sent, wait)
        }
        Slave(host, port) -> Slave(host, port)
      }

      // Reset queue
      let replication_queue = ReplicationQueue([], timestamp.system_time())
      DatabaseState(
        ..state,
        replication_state: new_replication_state,
        replication_queue:,
      )
    }
  }
}

// Start background timer to flush replication queue periodically
fn start_flush_timer(subject: Subject(Message)) -> Nil {
  process.start(
    fn() {
      process.sleep(replication_max_delay_ms)
      process.send(subject, AsyncMessage(FlushReplication))
      start_flush_timer(subject)
      // Recursive timer
    },
    True,
  )
  Nil
}

fn command_to_propagation_payload_internal(
  key: BitArray,
  value: resp.Resp,
  duration_ms: option.Option(Int),
) -> option.Option(BitArray) {
  case value {
    resp.BulkString(val) -> {
      let base_args = [
        resp.BulkString(bit_array.from_string("SET")),
        resp.BulkString(key),
        resp.BulkString(val),
      ]
      let args = case duration_ms {
        option.Some(ms) -> {
          let extras = [
            resp.BulkString(bit_array.from_string("PX")),
            resp.BulkString(bit_array.from_string(int.to_string(ms))),
          ]
          list.append(base_args, extras)
        }
        option.None -> base_args
      }
      resp.Array(args) |> resp.to_bit_array |> option.Some
    }
    _ -> option.None
  }
}

fn send_rdb_to_slave(state: DatabaseState, sender: Subject(resp.Resp)) {
  case state.replication_state {
    Master(subjects_registry, _, _, _) -> {
      let pid = process.subject_owner(sender)
      let stream_subject = dict.get(subjects_registry, pid)
      let playload = <<"$88\r\n":utf8, empty_rdb_hex:size({ 88 * 8 })>>
      stream_subject
      |> result.map(fn(sub) { process.send(sub, playload) })
      |> result.unwrap_both
    }
    Slave(_, _) -> Nil
  }
}

fn send_payload_to_slaves(
  slaves: List(process.Subject(BitArray)),
  payload: BitArray,
) {
  list.each(slaves, fn(slave) { process.send(slave, payload) })
}

fn send_getack_to_slaves(slaves: List(process.Subject(BitArray))) -> Nil {
  let getack_payload =
    resp.Array([
      resp.BulkString(<<"REPLCONF":utf8>>),
      resp.BulkString(<<"GETACK":utf8>>),
      resp.BulkString(<<"*":utf8>>),
    ])
    |> resp.to_bit_array()

  list.each(slaves, fn(slave) { process.send(slave, getack_payload) })
}
