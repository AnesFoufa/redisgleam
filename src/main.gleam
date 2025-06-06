import database
import gleam/bytes_tree
import gleam/io
import gleam/result
import gleam/string

import gleam/erlang/process
import gleam/option.{None}
import gleam/otp/actor
import glisten
import resp.{from_bit_array}

pub fn main() {
  let assert Ok(db) = database.start()
  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, fn(msg, state, conn) {
      io.println("Received message!")
      let assert glisten.Packet(msg) = msg
      let input = from_bit_array(msg)
      let response =
        input
        |> result.map(parse_command)
        |> result.map_error(resp.SimpleError)
        |> result.flatten()
        |> result.map(fn(command) { handle_command(db, command) })
        |> result.unwrap_both()
        |> resp.to_bit_array()
        |> bytes_tree.from_bit_array()

      let assert Ok(_) = glisten.send(conn, response)
      actor.continue(state)
    })
    |> glisten.serve(6379)

  process.sleep_forever()
}

type Command {
  Ping
  Echo(resp.Resp)
  Get(key: String)
  Set(key: String, value: resp.Resp)
}

fn parse_command(input: resp.Resp) -> Result(Command, resp.Resp) {
  use #(command_name, args) <- result.try(case input {
    resp.Array([resp.BulkString(command_name), ..args]) ->
      Ok(#(command_name, args))
    _ -> resp.SimpleError("Expected a non empty array") |> Error
  })
  case command_name |> string.lowercase(), args {
    "ping", [] -> Ok(Ping)
    "echo", [value] -> Ok(Echo(value))
    "get", [resp.BulkString(key)] -> Ok(Get(key))
    "set", [resp.BulkString(key), value] -> Ok(Set(key, value))
    _, _ -> Error(resp.SimpleError("Unknown error"))
  }
}

fn handle_command(db: database.Database, command: Command) -> resp.Resp {
  case command {
    Ping -> resp.SimpleString("PONG")
    Echo(value) -> value
    Get(key) -> database.get(db, key)
    Set(key, value) -> database.set(db, key, value)
  }
}
