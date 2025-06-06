import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/otp/actor.{type StartError}
import gleam/result
import resp

pub opaque type Database {
  Database(inner: Subject(Message))
}

pub fn start() -> Result(Database, StartError) {
  use subject <- result.map(actor.start(dict.new(), message_handler))
  Database(inner: subject)
}

pub fn get(db: Database, key: String) -> resp.Resp {
  process.call_forever(db.inner, message(Get(key)))
}

pub fn set(db: Database, key: String, value: resp.Resp) -> resp.Resp {
  process.call_forever(db.inner, message(Set(key, value)))
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
  Get(key: String)
  Set(key: String, value: resp.Resp)
}

fn message_handler(message: Message, state: dict.Dict(String, resp.Resp)) {
  let #(response, state) = case message.command {
    Get(key) -> {
      let response = state |> dict.get(key) |> result.unwrap(or: resp.Null)
      #(response, state)
    }
    Set(key, value) -> {
      let state = state |> dict.insert(key, value)
      let response = resp.SimpleString("OK")
      #(response, state)
    }
  }
  process.send(message.sender, response)
  actor.continue(state)
}
