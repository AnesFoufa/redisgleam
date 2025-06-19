import file_streams/file_stream
import filepath
import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option
import gleam/order
import gleam/otp/actor
import gleam/result
import gleam/time/duration
import gleam/time/timestamp
import rdb.{type Item}
import resp

pub opaque type Database {
  Database(inner: Subject(Message))
}

pub type Config {
  Config(dir: String, db_filename: String, port: Int)
}

pub fn start(config: Config) -> Database {
  let assert Ok(subject) = actor.start(dict.new(), message_handler)
  read_data_from_file(subject, config)
  Database(inner: subject)
}

fn read_data_from_file(subject: Subject(Message), config: Config) {
  process.start(
    fn() {
      let path = filepath.join(config.dir, config.db_filename)
      let assert Ok(stream) = file_stream.open_read(path)
      let assert Ok(content) = file_stream.read_remaining_bytes(stream)
      let assert Ok(rdb) = rdb.from_bit_array(content)
      let assert Ok(data) = rdb.databases |> list.first
      process.call_forever(subject, message(UpdataData(data)))
    },
    False,
  )
}

pub fn get(db: Database, key: BitArray) -> resp.Resp {
  process.call_forever(db.inner, message(Get(key)))
}

pub fn set(
  db: Database,
  key: BitArray,
  value: resp.Resp,
  duration: option.Option(Int),
) -> resp.Resp {
  let duration =
    duration
    |> option.map(fn(x) { int.max(x, 0) })
    |> option.map(duration.milliseconds)
  process.call_forever(db.inner, message(Set(key, value, duration)))
}

pub fn keys(db: Database) -> resp.Resp {
  process.call_forever(db.inner, message(Keys))
}

pub fn update_data(db: Database, data: dict.Dict(BitArray, Item)) {
  process.call_forever(db.inner, message(UpdataData(data)))
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
  )
  Keys
  UpdataData(data: dict.Dict(BitArray, Item))
}

fn message_handler(message: Message, state: dict.Dict(BitArray, Item)) {
  let #(response, state) = case message.command {
    Get(key) -> handle_get(key, state)
    Set(key, value, duration) -> {
      let expires_at =
        duration
        |> option.map(fn(d) { timestamp.add(timestamp.system_time(), d) })
      let item = rdb.Item(value, expires_at)
      let state = state |> dict.insert(key, item)
      let response = resp.SimpleString(<<"OK">>)
      #(response, state)
    }
    Keys -> {
      let response =
        dict.keys(state)
        |> list.map(resp.BulkString)
        |> resp.Array
      #(response, state)
    }
    UpdataData(data) -> {
      let response = resp.SimpleString(<<"OK">>)
      let state = dict.merge(data, state)
      #(response, state)
    }
  }
  process.send(message.sender, response)
  actor.continue(state)
}

fn handle_get(
  key: BitArray,
  state: dict.Dict(BitArray, Item),
) -> #(resp.Resp, dict.Dict(BitArray, Item)) {
  let #(response, state) =
    state
    |> dict.get(key)
    |> result.map(fn(item) {
      case item.expires_at {
        option.None -> #(item.value, state)
        option.Some(expires_at) -> {
          case timestamp.compare(timestamp.system_time(), expires_at) {
            order.Gt -> {
              let state = dict.delete(state, key)
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
