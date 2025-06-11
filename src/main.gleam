import database
import gleam/bit_array
import gleam/bytes_tree
import gleam/int
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
  Get(key: BitArray)
  Set(key: BitArray, value: resp.Resp, duration: option.Option(Int))
}

fn parse_command(input: resp.Resp) -> Result(Command, resp.Resp) {
  use #(command_name, args) <- result.try(case input {
    resp.Array([resp.BulkString(command_name), ..args]) ->
      Ok(#(command_name, args))
    _ -> resp.SimpleError(<<"Expected a non empty array">>) |> Error
  })
  use command_name <- result.try(
    bit_array.to_string(command_name)
    |> result.map_error(fn(_) {
      resp.SimpleError(<<"Expected a valid string as command name">>)
    }),
  )
  case command_name |> string.lowercase(), args {
    "ping", [] -> Ok(Ping)
    "echo", [value] -> Ok(Echo(value))
    "get", [resp.BulkString(key)] -> Ok(Get(key))
    "set", [resp.BulkString(key), value] -> Ok(Set(key, value, None))
    "set",
      [
        resp.BulkString(key),
        value,
        resp.BulkString(<<"px">>),
        resp.BulkString(duration),
      ]
    -> {
      let duration =
        duration
        |> bit_array.to_string
        |> result.try(int.parse)
        |> option.from_result
      Ok(Set(key, value, duration))
    }
    _, _ -> Error(resp.SimpleError(<<"Unknown error">>))
  }
}

fn handle_command(db: database.Database, command: Command) -> resp.Resp {
  case command {
    Ping -> resp.SimpleString(<<"PONG">>)
    Echo(value) -> value
    Get(key) -> database.get(db, key)
    Set(key, value, duration) -> database.set(db, key, value, duration)
  }
}
