import command
import file_streams/file_stream
import filepath
import gleam/bit_array
import gleam/dict
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
import resp.{type Resp}

pub opaque type Database {
  Database(inner: Subject(Message), config: Config)
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
  let state = DatabaseState(dict.new(), replication_state)
  let assert Ok(subject) = actor.start(state, message_handler)
  read_data_from_file(subject, config)
  Database(inner: subject, config:)
}

pub fn handle_command(db: Database, cmd: command.Command) -> Resp {
  let repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
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
            <> repl_id
            <> "\r\nmaster_repl_offset:0",
          ))
      }
    }
    command.ReplConf(_args) -> resp.SimpleString(<<"OK">>)
    command.Psync -> psync(db)
    command.Keys -> keys(db)
  }
}

/// Apply a command silently (no response). Used for processing propagated commands from master.
pub fn apply_command_silent(db: Database, cmd: command.Command) -> Nil {
  case cmd {
    command.Set(key, value, duration) -> {
      set(db, key, value, duration)
      Nil
    }
    // Other write commands can be added here as needed
    _ -> Nil
  }
}

pub fn update_data(db: Database, data: dict.Dict(BitArray, Item)) {
  process.call_forever(db.inner, message(UpdateDate(data)))
}

pub fn register(db: Database, subject) {
  process.call_forever(db.inner, message(Register(subject)))
}

fn get(db: Database, key: BitArray) -> resp.Resp {
  process.call_forever(db.inner, message(Get(key)))
}

fn set(
  db: Database,
  key: BitArray,
  value: resp.Resp,
  duration: option.Option(Int),
) -> resp.Resp {
  let duration_ms = duration |> option.map(fn(x) { int.max(x, 0) })
  let duration =
    duration_ms
    |> option.map(duration.milliseconds)
  process.call_forever(
    db.inner,
    message(Set(key, value, duration, duration_ms)),
  )
}

fn keys(db: Database) -> resp.Resp {
  process.call_forever(db.inner, message(Keys))
}

fn psync(db: Database) {
  process.call_forever(db.inner, message(Psync))
}

type ReplicationState {
  Master(
    subjects_registry: dict.Dict(process.Pid, process.Subject(BitArray)),
    slaves: List(process.Subject(BitArray)),
  )
  Slave(master_host: String, master_port: Int)
}

fn master() -> ReplicationState {
  Master(dict.new(), [])
}

type DatabaseState {
  DatabaseState(
    data: dict.Dict(BitArray, Item),
    replication_state: ReplicationState,
  )
}

fn read_data_from_file(subject: Subject(Message), config: Config) {
  process.start(
    fn() {
      let path = filepath.join(config.dir, config.db_filename)
      let assert Ok(stream) = file_stream.open_read(path)
      let assert Ok(content) = file_stream.read_remaining_bytes(stream)
      let assert Ok(rdb) = rdb.from_bit_array(content)
      let assert Ok(data) = rdb.databases |> list.first
      process.call_forever(subject, message(UpdateDate(data)))
    },
    False,
  )
}

type Message {
  Message(command: Command, sender: Subject(resp.Resp))
}

fn message(command: Command) {
  let res = fn(reply_with: Subject(resp.Resp)) {
    Message(command: command, sender: reply_with)
  }
  res
}

type Command {
  Get(key: BitArray)
  Set(
    key: BitArray,
    value: resp.Resp,
    duration: option.Option(duration.Duration),
    duration_ms: option.Option(Int),
  )
  Keys
  UpdateDate(data: dict.Dict(BitArray, Item))
  Register(subject: process.Subject(BitArray))
  Psync
}

fn message_handler(message: Message, state: DatabaseState) {
  let #(response, state) = case message.command {
    Get(key) -> handle_get(key, state)
    Set(key, value, duration, _duration_ms) -> {
      let expires_at =
        duration
        |> option.map(fn(d) { timestamp.add(timestamp.system_time(), d) })
      let item = rdb.Item(value, expires_at)
      let data = state.data |> dict.insert(key, item)
      let state = DatabaseState(..state, data: data)
      let response = resp.SimpleString(<<"OK">>)
      #(response, state)
    }
    Keys -> {
      let response =
        dict.keys(state.data)
        |> list.map(resp.BulkString)
        |> resp.Array
      #(response, state)
    }
    UpdateDate(incoming_data) -> {
      let response = resp.SimpleString(<<"OK">>)
      let data = dict.merge(incoming_data, state.data)
      let state = DatabaseState(..state, data:)
      #(response, state)
    }
    Register(subject) -> {
      let response = resp.SimpleString(<<"Ok">>)
      case state.replication_state {
        Master(_, _) as m -> {
          let owner = process.subject_owner(subject)
          let subjects_registry =
            dict.insert(m.subjects_registry, owner, subject)
          let replication_state = Master(..m, subjects_registry:)
          let state = DatabaseState(..state, replication_state:)
          #(response, state)
        }
        Slave(_, _) -> #(response, state)
      }
    }
    Psync -> {
      let repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
      let replication_state = case state.replication_state {
        Master(subjects_registry, slaves) -> {
          let pid = process.subject_owner(message.sender)
          let new_slaves = case dict.get(subjects_registry, pid) {
            Ok(subject) -> [subject, ..slaves]
            Error(_) -> slaves
          }
          Master(subjects_registry:, slaves: new_slaves)
        }
        Slave(master_host, master_port) -> Slave(master_host, master_port)
      }
      let state = DatabaseState(..state, replication_state:)
      let response =
        resp.SimpleString(bit_array.from_string(
          "FULLRESYNC " <> repl_id <> " 0",
        ))
      #(response, state)
    }
  }
  process.send(message.sender, response)
  case message.command {
    Psync -> send_rdb_to_slave(state, message.sender)
    Set(_, _, _, _) -> propagate_write_command(message.command, state)
    _ -> Nil
  }
  actor.continue(state)
}

fn handle_get(
  key: BitArray,
  state: DatabaseState,
) -> #(resp.Resp, DatabaseState) {
  let #(response, state) =
    state.data
    |> dict.get(key)
    |> result.map(fn(item) {
      case item.expires_at {
        option.None -> #(item.value, state)
        option.Some(expires_at) -> {
          case timestamp.compare(timestamp.system_time(), expires_at) {
            order.Gt -> {
              let data = dict.delete(state.data, key)
              let state = DatabaseState(..state, data:)
              #(resp.Null, state)
            }
            _ -> #(item.value, state)
          }
        }
      }
    })
    |> result.unwrap(or: #(resp.Null, state))
  #(response, state)
}

fn send_rdb_to_slave(state: DatabaseState, sender: Subject(resp.Resp)) {
  case state.replication_state {
    Master(subjects_registry, _) -> {
      let pid = process.subject_owner(sender)
      let stream_subject = dict.get(subjects_registry, pid)
      let rdb =
        0x524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2
      let playload = <<"$88\r\n":utf8, rdb:size({ 88 * 8 })>>
      stream_subject
      |> result.map(fn(sub) { process.send(sub, playload) })
      |> result.unwrap_both
    }
    Slave(_, _) -> Nil
  }
}

fn propagate_write_command(command: Command, state: DatabaseState) {
  case command_to_propagation_payload(command) {
    option.Some(payload) -> {
      case state.replication_state {
        Master(_, slaves) -> send_payload_to_slaves(slaves, payload)
        Slave(_, _) -> Nil
      }
    }
    option.None -> Nil
  }
}

fn send_payload_to_slaves(
  slaves: List(process.Subject(BitArray)),
  payload: BitArray,
) {
  list.each(slaves, fn(slave) { process.send(slave, payload) })
}

fn command_to_propagation_payload(command: Command) -> option.Option(BitArray) {
  case command {
    Set(key, resp.BulkString(value), _, duration_ms) -> {
      let base_args = [
        resp.BulkString(bit_array.from_string("SET")),
        resp.BulkString(key),
        resp.BulkString(value),
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
      resp.Array(args)
      |> resp.to_bit_array
      |> option.Some
    }
    Set(_, _, _, _) -> option.None
    _ -> option.None
  }
}
