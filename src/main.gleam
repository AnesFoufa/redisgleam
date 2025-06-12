import argv
import clad
import database
import gleam/bit_array
import gleam/bytes_tree
import gleam/int
import gleam/io
import gleam/result
import gleam/string

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/option.{None}
import gleam/otp/actor
import glisten
import resp.{from_bit_array}

pub fn main() {
  let config_decoder = {
    use dir <- decode.optional_field("dir", "/var/lib/redis", decode.string)
    use db_filename <- decode.optional_field(
      "dbfilename",
      "dump.rdb",
      decode.string,
    )
    decode.success(Config(dir:, db_filename:))
  }
  let assert Ok(config) = clad.decode(argv.load().arguments, config_decoder)
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
        |> result.map(fn(command) { handle_command(config, db, command) })
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
  GetConfigDir
  GetConfigDbFileName
}

type Config {
  Config(dir: String, db_filename: String)
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
    "config", [resp.BulkString(config_command), ..args] -> {
      case bit_array.to_string(config_command) {
        Ok(command_name) -> parse_config_command(command_name, args)
        Error(_) ->
          Error(resp.SimpleError(<<"Config command not a string!!!">>))
      }
    }
    _, _ -> Error(resp.SimpleError(<<"Unknown command">>))
  }
}

fn parse_config_command(
  command_name: String,
  args: List(resp.Resp),
) -> Result(Command, resp.Resp) {
  case string.lowercase(command_name), args {
    "get", [resp.BulkString(config_parameter)] -> {
      use config_parameter_name <- result.try(
        config_parameter
        |> bit_array.to_string()
        |> result.replace_error(
          resp.SimpleError(<<"Config parameter not a string">>),
        ),
      )
      case string.lowercase(config_parameter_name) {
        "dir" -> Ok(GetConfigDir)
        "db_filename" -> Ok(GetConfigDbFileName)
        _ -> Error(resp.SimpleError(<<"Unexpected config parameter">>))
      }
    }
    _, _ -> Error(resp.SimpleError(<<"Unknown config command">>))
  }
}

fn handle_command(
  config: Config,
  db: database.Database,
  command: Command,
) -> resp.Resp {
  case command {
    Ping -> resp.SimpleString(<<"PONG">>)
    Echo(value) -> value
    Get(key) -> database.get(db, key)
    Set(key, value, duration) -> database.set(db, key, value, duration)
    GetConfigDbFileName ->
      resp.Array([
        resp.BulkString(<<"db_filename">>),
        resp.BulkString(bit_array.from_string(config.db_filename)),
      ])
    GetConfigDir ->
      resp.Array([
        resp.BulkString(<<"dir">>),
        resp.BulkString(bit_array.from_string(config.dir)),
      ])
  }
}
